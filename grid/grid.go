package grid

import (
	"context"
	"fmt"
	"github.com/huobirdcenter/huobi_golang/logging/applogger"
	"github.com/huobirdcenter/huobi_golang/pkg/response/order"
	"github.com/shopspring/decimal"
	"github.com/xyths/hs"
	"github.com/xyths/hs/exchange/huobi"
	"github.com/xyths/hs/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
)

type StrategyConf struct {
	MaxPrice float64
	MinPrice float64
	Number   int
	Total    float64
}

type Config struct {
	Exchange hs.ExchangeConf
	Mongo    hs.MongoConf
	Strategy StrategyConf
}

type Trader struct {
	config Config

	mdb *mongo.Database

	ex              *huobi.Client
	symbol          string
	baseCurrency    string
	quoteCurrency   string
	pricePrecision  int32
	amountPrecision int32
	minAmount       decimal.Decimal
	minTotal        decimal.Decimal

	scale  decimal.Decimal
	grids  []hs.Grid
	base   int
	cost   decimal.Decimal // average price
	amount decimal.Decimal // amount held
}

func New(configFilename string) *Trader {
	cfg := Config{}
	if err := hs.ParseJsonConfig(configFilename, &cfg); err != nil {
		log.Fatal(err)
	}
	return &Trader{
		config: cfg,
	}
}

func (t *Trader) Init(ctx context.Context) {
	t.initMongo(ctx)
	t.initEx(ctx)
	t.initGrids(ctx)
}

func (t *Trader) Trade(ctx context.Context) error {
	_ = t.Print(ctx)
	clientId := fmt.Sprintf("%d", time.Now().Unix())
	// subscribe all event
	go t.ex.SubscribeOrder(ctx, t.symbol, clientId, t.OrderUpdateHandler)
	go t.ex.SubscribeTradeClear(ctx, t.symbol, clientId, t.TradeClearHandler)
	// rebalance
	if err := t.ReBalance(ctx); err != nil {
		log.Fatalf("error when rebalance: %s", err)
	}

	// setup all grid orders
	t.setupGridOrders(ctx)

	quit := make(chan os.Signal)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Info("Trade is now stopping...")

	return nil
}

// print grid to stdout
func (t *Trader) Print(ctx context.Context) error {
	delta, _ := t.scale.Float64()
	delta = 1 - delta
	log.Infof("Scale is %s (%1.2f%%)", t.scale.String(), 100*delta)
	log.Infof("Id\tTotal\tPrice\tAmountBuy\tAmountSell")
	for _, g := range t.grids {
		log.Infof("%2d\t%s\t%s\t%s\t%s",
			g.Id, g.TotalBuy.StringFixed(t.amountPrecision+t.pricePrecision),
			g.Price.StringFixed(t.pricePrecision),
			g.AmountBuy.StringFixed(t.amountPrecision), g.AmountSell.StringFixed(t.amountPrecision))
	}

	return nil
}

func (t *Trader) Close(ctx context.Context) {
	_ = t.mdb.Client().Disconnect(ctx)
	t.cancelAllOrders(ctx)
}

func (t *Trader) initMongo(ctx context.Context) {
	clientOpts := options.Client().ApplyURI(t.config.Mongo.URI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatal("Error when connect to mongo:", err)
	}
	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Error when ping to mongo:", err)
	}
	t.mdb = client.Database(t.config.Mongo.Database)
}

func (t *Trader) initEx(ctx context.Context) {
	t.ex = huobi.New(t.config.Exchange.Label, t.config.Exchange.Key, t.config.Exchange.Secret, t.config.Exchange.Host)
	switch t.config.Exchange.Symbols {
	case "btc_usdt":
		t.symbol = huobi.BTC_USDT
		t.baseCurrency = "btc"
		t.quoteCurrency = "usdt"
	default:
		t.symbol = "btc_usdt"
	}
	t.pricePrecision = int32(huobi.PricePrecision[t.symbol])
	t.amountPrecision = int32(huobi.AmountPrecision[t.symbol])
	t.minAmount = decimal.NewFromFloat(huobi.MinAmount[t.symbol])
	t.minTotal = decimal.NewFromInt(huobi.MinTotal[t.symbol])
	//log.Debugf("init ex, pricePrecision = %d, amountPrecision = %d, minAmount = %s, minTotal = %s",
	//	t.pricePrecision, t.amountPrecision, t.minAmount.String(), t.minTotal.String())
}

func (t *Trader) initGrids(ctx context.Context) {
	maxPrice := t.config.Strategy.MaxPrice
	minPrice := t.config.Strategy.MinPrice
	number := t.config.Strategy.Number
	total := t.config.Strategy.Total
	//log.Debugf("init grids, MaxPrice: %f, MinPrice: %f, Grid Number: %d, total: %f",
	//	maxPrice, minPrice, number, total)
	t.scale = decimal.NewFromFloat(math.Pow(minPrice/maxPrice, 1.0/float64(number)))
	preTotal := decimal.NewFromFloat(total / float64(number))
	currentPrice := decimal.NewFromFloat(maxPrice)
	currentGrid := hs.Grid{
		Id:    0,
		Price: currentPrice.Round(t.pricePrecision),
	}
	t.grids = append(t.grids, currentGrid)
	for i := 1; i <= number; i++ {
		currentPrice = currentPrice.Mul(t.scale).Round(t.pricePrecision)
		amountBuy := preTotal.Div(currentPrice).Round(t.amountPrecision)
		if amountBuy.Cmp(t.minAmount) == -1 {
			log.Fatalf("amount %s less than minAmount(%s)", amountBuy, t.minAmount)
		}
		realTotal := currentPrice.Mul(amountBuy)
		if realTotal.Cmp(t.minTotal) == -1 {
			log.Fatalf("total %s less than minTotal(%s)", realTotal, t.minTotal)
		}
		currentGrid = hs.Grid{
			Id:        i,
			Price:     currentPrice,
			AmountBuy: amountBuy,
			TotalBuy:  realTotal,
		}
		t.grids = append(t.grids, currentGrid)
		t.grids[i-1].AmountSell = amountBuy
	}
}

func (t *Trader) ReBalance(ctx context.Context) error {
	price, err := t.ex.GetPrice(t.symbol)
	if err != nil {
		return err
	}
	t.base = 0
	moneyNeed := decimal.NewFromInt(0)
	coinNeed := decimal.NewFromInt(0)
	for i, g := range t.grids {
		if g.Price.Cmp(price) == 1 {
			t.base = i
			coinNeed = coinNeed.Add(g.AmountBuy)
		} else {
			moneyNeed = moneyNeed.Add(g.TotalBuy)
		}
	}
	log.Infof("now base = %d, moneyNeed = %s, coinNeed = %s", t.base, moneyNeed, coinNeed)
	balance, err := t.ex.GetSpotBalance()
	if err != nil {
		log.Fatalf("error when get balance in rebalance: %s", err)
	}
	moneyHeld := balance[t.quoteCurrency]
	coinHeld := balance[t.baseCurrency]
	log.Infof("account has money %s, coin %s", moneyHeld, coinHeld)
	t.cost = price
	t.amount = coinNeed
	direct, amount := t.assetRebalancing(moneyNeed, coinNeed, moneyHeld, coinHeld, price)
	if direct == -2 || direct == 2 {
		log.Fatalf("no enough money for rebalance, direct: %d", direct)
	} else if direct == 0 {
		log.Info("no need to rebalance")
	} else if direct == -1 {
		// place sell order
		t.base++
		clientOrderId := fmt.Sprintf("p-s-%d", time.Now().Unix())
		orderId, err := t.sell(clientOrderId, price, amount)
		if err != nil {
			log.Fatalf("error when rebalance: %s", err)
		}
		log.Debugf("rebalance: sell %s coin at price %s, orderId is %d, clientOrderId is %s",
			amount, price, orderId, clientOrderId)
	} else if direct == 1 {
		// place buy order
		clientOrderId := fmt.Sprintf("p-b-%d", time.Now().Unix())
		orderId, err := t.buy(clientOrderId, price, amount)
		if err != nil {
			log.Fatalf("error when rebalance: %s", err)
		}
		log.Debugf("rebalance: buy %s coin at price %s, orderId is %d, clientOrderId is %s",
			amount, price, orderId, clientOrderId)
	}

	return nil
}

func (t *Trader) OrderUpdateHandler(response interface{}) {
	subOrderResponse, ok := response.(order.SubscribeOrderV2Response)
	if !ok {
		log.Warnf("Received unknown response: %v", response)
	}
	//log.Printf("subOrderResponse = %#v", subOrderResponse)
	if subOrderResponse.Action == "sub" {
		if subOrderResponse.IsSuccess() {
			log.Infof("Subscription topic %s successfully", subOrderResponse.Ch)
		} else {
			log.Fatalf("Subscription topic %s error, code: %d, message: %s",
				subOrderResponse.Ch, subOrderResponse.Code, subOrderResponse.Message)
		}
	} else if subOrderResponse.Action == "push" {
		if subOrderResponse.Data == nil {
			log.Infof("SubscribeOrderV2Response has no data: %#v", subOrderResponse)
			return
		}
		o := subOrderResponse.Data
		//log.Printf("Order update, event: %s, symbol: %s, type: %s, id: %d, clientId: %s, status: %s",
		//	o.EventType, o.Symbol, o.Type, o.OrderId, o.ClientOrderId, o.OrderStatus)
		switch o.EventType {
		case "creation":
			log.Debugf("order created, orderId: %d, clientOrderId: %s", o.OrderId, o.ClientOrderId)
		case "cancellation":
			log.Debugf("order cancelled, orderId: %d, clientOrderId: %s", o.OrderId, o.ClientOrderId)
		case "trade":
			log.Debugf("order filled, orderId: %d, clientOrderId: %s, fill type: %s", o.OrderId, o.ClientOrderId, o.OrderStatus)
			go t.processOrderTrade(o.TradeId, uint64(o.OrderId), o.ClientOrderId, o.OrderStatus, o.TradePrice, o.TradeVolume, o.RemainAmt)
		default:
			log.Warnf("unknown eventType, should never happen, orderId: %d, clientOrderId: %s, eventType: %s",
				o.OrderId, o.ClientOrderId, o.EventType)
		}
	}
}

func (t *Trader) TradeClearHandler(response interface{}) {
	subResponse, ok := response.(order.SubscribeTradeClearResponse)
	if ok {
		if subResponse.Action == "sub" {
			if subResponse.IsSuccess() {
				applogger.Info("Subscription TradeClear topic %s successfully", subResponse.Ch)
			} else {
				applogger.Error("Subscription TradeClear topic %s error, code: %d, message: %s",
					subResponse.Ch, subResponse.Code, subResponse.Message)
			}
		} else if subResponse.Action == "push" {
			if subResponse.Data == nil {
				log.Infof("SubscribeOrderV2Response has no data: %#v", subResponse)
				return
			}
			o := subResponse.Data
			trade := huobi.Trade{
				Symbol:            subResponse.Data.Symbol,
				OrderId:           subResponse.Data.OrderId,
				OrderType:         subResponse.Data.OrderType,
				Aggressor:         subResponse.Data.Aggressor,
				Id:                subResponse.Data.TradeId,
				Time:              subResponse.Data.TradeTime,
				Price:             decimal.RequireFromString(subResponse.Data.TradePrice),
				Volume:            decimal.RequireFromString(subResponse.Data.TradeVolume),
				TransactFee:       decimal.RequireFromString(subResponse.Data.TransactFee),
				FeeDeduct:         decimal.RequireFromString(subResponse.Data.FeeDeduct),
				FeeDeductCurrency: subResponse.Data.FeeDeductType,
			}
			applogger.Info("Order update, symbol: %s, order id: %d, price: %s, volume: %s",
				o.Symbol, o.OrderId, o.TradePrice, o.TradeVolume)
			go t.processClearTrade(trade)
		}
	} else {
		applogger.Warn("Received unknown response: %v", response)
	}

}

func (t *Trader) processOrderTrade(tradeId int64, orderId uint64, clientOrderId, orderStatus, tradePrice, tradeVolume, remainAmount string) {
	log.Sugar.Debugw("process order trade",
		"tradeId", tradeId,
		"orderId", orderId,
		"clientOrderId", clientOrderId,
		"orderStatus", orderStatus,
		"tradePrice", tradePrice,
		"tradeVolume", tradeVolume,
		"remainAmount", remainAmount)
	if strings.HasPrefix(clientOrderId, "p-") {
		log.Infof("rebalance order filled/partial-filled, tradeId: %d, orderId: %d, clientOrderId: %s", tradeId, orderId, clientOrderId)
		return
	}
	// update grid
	remain, err := decimal.NewFromString(remainAmount)
	if err != nil {
		log.Sugar.Errorw("Error when prase remainAmount",
			"error", err,
			"remainAmount", remainAmount)
	}
	if !remain.Equal(decimal.NewFromInt(0)) && orderStatus != "filled" {
		log.Sugar.Infow("not full-filled order",
			"orderStatus", orderStatus,
			"remainAmount", remainAmount)
		return
	}
	if strings.HasPrefix(clientOrderId, "b-") {
		// buy order filled
		if orderId != t.grids[t.base+1].Order {
			log.Errorf("[ERROR] buy order position is NOT the base+1, base: %d", t.base)
			return
		}
		t.grids[t.base+1].Order = 0
		t.down()
	} else if strings.HasPrefix(clientOrderId, "s-") {
		// sell order filled
		if orderId != t.grids[t.base-1].Order {
			log.Errorf("[ERROR] sell order position is NOT the base+1, base: %d", t.base)
			return
		}
		t.grids[t.base-1].Order = 0
		t.up()
	} else {
		log.Warnf("I don't know the clientOrderId: %s, maybe it's a manual order: %s", clientOrderId, orderId)
	}
}

func (t *Trader) processClearTrade(trade huobi.Trade) {
	log.Sugar.Debugw("process trade clear",
		"tradeId", trade.Id,
		"orderId", trade.OrderId,
		"orderType", trade.OrderType,
		"price", trade.Price,
		"volume", trade.Volume,
		"transactFee", trade.TransactFee,
		"feeDeduct", trade.FeeDeduct,
		"feeDeductCurrency", trade.FeeDeductCurrency,
	)
	oldTotal := t.amount.Mul(t.cost)
	if trade.OrderType == huobi.OrderTypeSellLimit {
		trade.Volume = trade.Volume.Neg()
	}
	t.amount = t.amount.Add(trade.Volume)
	tradeTotal := trade.Volume.Mul(trade.Price)
	newTotal := oldTotal.Add(tradeTotal)
	t.cost = newTotal.Div(t.amount)
	log.Sugar.Infow("Average cost update", "cost", t.cost)
}

func (t *Trader) buy(clientOrderId string, price, amount decimal.Decimal) (uint64, error) {
	log.Infof("[Order][buy] price: %s, amount: %s", price, amount)
	return t.ex.PlaceOrder(huobi.OrderTypeBuyLimit, t.symbol, clientOrderId, price, amount)
}

func (t *Trader) sell(clientOrderId string, price, amount decimal.Decimal) (uint64, error) {
	log.Infof("[Order][sell] price: %s, amount: %s", price, amount)
	return t.ex.PlaceOrder(huobi.OrderTypeSellLimit, t.symbol, clientOrderId, price, amount)
}

func (t *Trader) setupGridOrders(ctx context.Context) {
	for i := t.base - 1; i >= 0; i-- {
		// sell
		clientOrderId := fmt.Sprintf("s-%d-%d", i, time.Now().Unix())
		orderId, err := t.sell(clientOrderId, t.grids[i].Price, t.grids[i].AmountSell)
		if err != nil {
			log.Errorf("error when setupGridOrders, grid number: %d, err: %s", i, err)
			continue
		}
		t.grids[i].Order = orderId
	}
	for i := t.base + 1; i < len(t.grids); i++ {
		// buy
		clientOrderId := fmt.Sprintf("b-%d-%d", i, time.Now().Unix())
		orderId, err := t.buy(clientOrderId, t.grids[i].Price, t.grids[i].AmountBuy)
		if err != nil {
			log.Errorf("error when setupGridOrders, grid number: %d, err: %s", i, err)
			continue
		}
		t.grids[i].Order = orderId
	}
}

func (t *Trader) cancelAllOrders(ctx context.Context) {
	for i := 0; i < len(t.grids); i++ {
		if t.grids[i].Order == 0 {
			continue
		}
		if ret, err := t.ex.CancelOrder(t.grids[i].Order); err == nil {
			log.Infof("cancel order successful, orderId: %d", t.grids[i].Order)
			t.grids[i].Order = 0

		} else {
			log.Errorf("cancel order error: orderId: %d, return code: %d, err: %s", t.grids[i].Order, ret, err)
		}
	}
}

// 0: no need
// 1: buy
// -1: sell
// 2: no enough money
// -2: no enough coin
func (t *Trader) assetRebalancing(moneyNeed, coinNeed, moneyHeld, coinHeld, price decimal.Decimal) (direct int, amount decimal.Decimal) {
	if moneyNeed.GreaterThan(moneyHeld) {
		// sell coin
		moneyDelta := moneyNeed.Sub(moneyHeld)
		sellAmount := moneyDelta.Div(price).Round(t.amountPrecision)
		if coinHeld.Cmp(coinNeed.Add(sellAmount)) == -1 {
			log.Errorf("no enough coin for rebalance: need hold %s and sell %s (%s in total), only have %s",
				coinNeed, sellAmount, coinNeed.Add(sellAmount), coinHeld)
			direct = -2
			return
		}

		if sellAmount.LessThan(t.minAmount) {
			log.Errorf("sell amount %s less than minAmount(%s), won't sell", sellAmount, t.minAmount)
			direct = 0
			return
		}
		if t.minTotal.GreaterThan(price.Mul(sellAmount)) {
			log.Infof("sell total %s less than minTotal(%s), won't sell", price.Mul(sellAmount), t.minTotal)
			direct = 0
			return
		}
		direct = -1
		amount = sellAmount
	} else {
		// buy coin
		if coinNeed.LessThan(coinHeld) {
			log.Infof("no need to rebalance: need coin %s, has %s, need money %s, has %s",
				coinNeed, coinHeld, moneyNeed, moneyHeld)
			direct = 0
			return
		}
		coinDelta := coinNeed.Sub(coinHeld).Round(t.amountPrecision)
		buyTotal := coinDelta.Mul(price)
		if moneyHeld.LessThan(moneyNeed.Add(buyTotal)) {
			log.Fatalf("no enough money for rebalance: need hold %s and spend %s (%s in total)，only have %s",
				moneyNeed, buyTotal, moneyNeed.Add(buyTotal), moneyHeld)
			direct = 2
		}
		if coinDelta.LessThan(t.minAmount) {
			log.Errorf("buy amount %s less than minAmount(%s), won't sell", coinDelta, t.minAmount)
			direct = 0
			return
		}
		if buyTotal.LessThan(t.minTotal) {
			log.Errorf("buy total %s less than minTotal(%s), won't sell", buyTotal, t.minTotal)
			direct = 0
			return
		}
		direct = 1
		amount = coinDelta
	}
	return
}

func (t *Trader) up() {
	// make sure base >= 0
	if t.base == 0 {
		log.Infof("grid base = 0, up OUT")
		return
	}
	t.base--
	if t.base < len(t.grids)-2 {
		// place buy order
		clientOrderId := fmt.Sprintf("b-%d-%d", t.base+1, time.Now().Unix())
		if orderId, err := t.buy(clientOrderId, t.grids[t.base+1].Price, t.grids[t.base+1].AmountBuy); err == nil {
			t.grids[t.base+1].Order = orderId
		}
	}
}

func (t *Trader) down() {
	// make sure base <= len(grids)
	if t.base == len(t.grids) {
		log.Infof("grid base = %d, down OUT", t.base)
		return
	}
	t.base++
	if t.base > 0 {
		// place sell order
		clientOrderId := fmt.Sprintf("s-%d-%d", t.base-1, time.Now().Unix())
		if orderId, err := t.sell(clientOrderId, t.grids[t.base-1].Price, t.grids[t.base-1].AmountSell); err == nil {
			t.grids[t.base-1].Order = orderId
		}
	}
}
