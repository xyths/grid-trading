package grid

import (
	"context"
	"fmt"
	"github.com/huobirdcenter/huobi_golang/pkg/response/order"
	"github.com/shopspring/decimal"
	"github.com/xyths/hs"
	"github.com/xyths/hs/exchange/huobi"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"log"
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

type Manager struct {
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

	grids        []Grid
	base         int
	averagePrice decimal.Decimal
	amountHeld   decimal.Decimal
}

func New(configFilename string) *Manager {
	cfg := Config{}
	if err := hs.ParseJsonConfig(configFilename, &cfg); err != nil {
		log.Fatal(err)
	}
	return &Manager{
		config: cfg,
	}
}

func (m *Manager) Init(ctx context.Context) {
	m.initMongo(ctx)
	m.initEx(ctx)
	m.initGrids(ctx)
}

func (m *Manager) Trade(ctx context.Context) error {
	clientId := fmt.Sprintf("%d", time.Now().Unix())
	// subscribe all event
	go m.ex.SubscribeOrder(ctx, m.symbol, clientId, m.OrderUpdateHandler)
	// rebalance
	if err := m.ReBalance(ctx); err != nil {
		log.Fatalf("error when rebalance: %s", err)
	}

	// setup all grid orders
	m.setupGridOrders(ctx)

	quit := make(chan os.Signal)
	// kill (no param) default send syscall.SIGTERM
	// kill -2 is syscall.SIGINT
	// kill -9 is syscall.SIGKILL but can't be catch, so don't need add it
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("Trade is now stopping...")

	return nil
}

func (m *Manager) Close(ctx context.Context) {
	_ = m.mdb.Client().Disconnect(ctx)
	m.cancelAllOrders(ctx)
}

func (m *Manager) initMongo(ctx context.Context) {
	clientOpts := options.Client().ApplyURI(m.config.Mongo.URI)
	client, err := mongo.Connect(ctx, clientOpts)
	if err != nil {
		log.Fatal("Error when connect to mongo:", err)
	}
	// Check the connection
	err = client.Ping(ctx, nil)
	if err != nil {
		log.Fatal("Error when ping to mongo:", err)
	}
	m.mdb = client.Database(m.config.Mongo.Database)
}

func (m *Manager) initEx(ctx context.Context) {
	m.ex = huobi.New(m.config.Exchange.Label, m.config.Exchange.Key, m.config.Exchange.Secret, m.config.Exchange.Host)
	switch m.config.Exchange.Currency {
	case "btc_usdt":
		m.symbol = huobi.BTC_USDT
		m.baseCurrency = "btc"
		m.quoteCurrency = "usdt"
	default:
		m.symbol = "btc_usdt"
	}
	m.pricePrecision = int32(huobi.PricePrecision[m.symbol])
	m.amountPrecision = int32(huobi.AmountPrecision[m.symbol])
	m.minAmount = decimal.NewFromFloat(huobi.MinAmount[m.symbol])
	m.minTotal = decimal.NewFromInt(huobi.MinTotal[m.symbol])
	log.Printf("init ex, pricePrecision = %d, amountPrecision = %d, minAmount = %s, minTotal = %s",
		m.pricePrecision, m.amountPrecision, m.minAmount.String(), m.minTotal.String())
}

func (m *Manager) initGrids(ctx context.Context) {
	maxPrice := m.config.Strategy.MaxPrice
	minPrice := m.config.Strategy.MinPrice
	number := m.config.Strategy.Number
	total := m.config.Strategy.Total
	log.Printf("init grids, MaxPrice: %f, MinPrice: %f, Grid Number: %d, total: %f",
		maxPrice, minPrice, number, total)
	scale := decimal.NewFromFloat(math.Pow(minPrice/maxPrice, 1.0/float64(number)))
	log.Printf("scale is %s", scale.String())
	preTotal := decimal.NewFromFloat(total / float64(number))
	currentPrice := decimal.NewFromFloat(maxPrice)
	currentGrid := Grid{
		Id:    0,
		Price: currentPrice.Round(m.pricePrecision),
	}
	m.grids = append(m.grids, currentGrid)
	for i := 1; i <= number; i++ {
		currentPrice = currentPrice.Mul(scale).Round(m.pricePrecision)
		amountBuy := preTotal.Div(currentPrice).Round(m.amountPrecision)
		if amountBuy.Cmp(m.minAmount) == -1 {
			log.Fatalf("amount %s less than minAmount(%s)", amountBuy, m.minAmount)
		}
		realTotal := currentPrice.Mul(amountBuy)
		if realTotal.Cmp(m.minTotal) == -1 {
			log.Fatalf("total %s less than minTotal(%s)", realTotal, m.minTotal)
		}
		currentGrid = Grid{
			Id:        i,
			Price:     currentPrice,
			AmountBuy: amountBuy,
			TotalBuy:  realTotal,
		}
		m.grids = append(m.grids, currentGrid)
		m.grids[i-1].AmountSell = amountBuy
	}
	log.Println("Id\tTotal\tPrice\tAmountBuy\tAmountSell")
	for _, g := range m.grids {
		log.Printf("%2d\t%s\t%s\t%s\t%s", g.Id, g.TotalBuy, g.Price, g.AmountBuy, g.AmountSell)
	}
	log.Println("finish init grid parameters")
}

func (m *Manager) ReBalance(ctx context.Context) error {
	price, err := m.ex.GetPrice(m.symbol)
	if err != nil {
		return err
	}
	m.base = 0
	moneyNeed := decimal.NewFromInt(0)
	coinNeed := decimal.NewFromInt(0)
	for i, g := range m.grids {
		if g.Price.Cmp(price) == 1 {
			m.base = i
			coinNeed = coinNeed.Add(g.AmountBuy)
		} else {
			moneyNeed = moneyNeed.Add(g.TotalBuy)
		}
	}
	log.Printf("now base = %d, moneyNeed = %s, coinNeed = %s", m.base, moneyNeed, coinNeed)
	balance, err := m.ex.GetSpotBalance()
	if err != nil {
		log.Fatalf("error when get balance in rebalance: %s", err)
	}
	moneyHeld := balance[m.quoteCurrency]
	coinHeld := balance[m.baseCurrency]
	log.Printf("account has money %s, coin %s", moneyHeld, coinHeld)
	m.averagePrice = price
	m.amountHeld = coinNeed
	direct, amount := m.assetRebalancing(moneyNeed, coinNeed, moneyHeld, coinHeld, price)
	if direct == -2 || direct == 2 {
		log.Fatalf("no enough money for rebalance, direct: %d", direct)
	} else if direct == 0 {
		log.Printf("no need to rebalance")
	} else if direct == -1 {
		// place sell order
		m.base++
		clientOrderId := fmt.Sprintf("p-s-%d", time.Now().Unix())
		orderId, err := m.sell(clientOrderId, price, amount)
		if err != nil {
			log.Fatalf("error when rebalance: %s", err)
		}
		log.Printf("rebalance: sell %s coin at price %s, orderId is %d, clientOrderId is %s",
			amount, price, orderId, clientOrderId)
	} else if direct == 1 {
		// place buy order
		clientOrderId := fmt.Sprintf("p-b-%d", time.Now().Unix())
		orderId, err := m.buy(clientOrderId, price, amount)
		if err != nil {
			log.Fatalf("error when rebalance: %s", err)
		}
		log.Printf("rebalance: buy %s coin at price %s, orderId is %d, clientOrderId is %s",
			amount, price, orderId, clientOrderId)
	}

	return nil
}

func (m *Manager) OrderUpdateHandler(response interface{}) {
	subOrderResponse, ok := response.(order.SubscribeOrderV2Response)
	if !ok {
		log.Printf("Received unknown response: %v", response)
	}
	//log.Printf("subOrderResponse = %#v", subOrderResponse)
	if subOrderResponse.Action == "sub" {
		if subOrderResponse.IsSuccess() {
			log.Printf("Subscription topic %s successfully", subOrderResponse.Ch)
		} else {
			log.Fatalf("Subscription topic %s error, code: %d, message: %s",
				subOrderResponse.Ch, subOrderResponse.Code, subOrderResponse.Message)
		}
	} else if subOrderResponse.Action == "push" {
		if subOrderResponse.Data == nil {
			log.Printf("SubscribeOrderV2Response has no data: %#v", subOrderResponse)
			return
		}
		o := subOrderResponse.Data
		//log.Printf("Order update, event: %s, symbol: %s, type: %s, id: %d, clientId: %s, status: %s",
		//	o.EventType, o.Symbol, o.Type, o.OrderId, o.ClientOrderId, o.OrderStatus)
		switch o.EventType {
		case "creation":
			log.Printf("order created, orderId: %d, clientOrderId: %s", o.OrderId, o.ClientOrderId)
		case "cancellation":
			log.Printf("order cancelled, orderId: %d, clientOrderId: %s", o.OrderId, o.ClientOrderId)
		case "trade":
			log.Printf("order filled, orderId: %d, clientOrderId: %s, fill type: %s", o.OrderId, o.ClientOrderId, o.OrderStatus)
			go m.processOrderTrade(o.TradeId, uint64(o.OrderId), o.ClientOrderId, o.OrderStatus, o.TradePrice, o.TradeVolume, o.RemainAmt)
		default:
			log.Printf("unknown eventType, should never happen, orderId: %d, clientOrderId: %s, eventType: %s",
				o.OrderId, o.ClientOrderId, o.EventType)
		}
	}
}

func (m *Manager) processOrderTrade(tradeId int64, orderId uint64, clientOrderId, orderStatus, tradePrice, tradeVolume, remainAmount string) {
	if strings.HasPrefix(clientOrderId, "p-") {
		log.Printf("rebalance order filled/partial-filled, tradeId: %d, orderId: %d, clientOrderId: %s", tradeId, orderId, clientOrderId)
		return
	}
	// update grid
	if remainAmount != "0" {
		return
	}
	if strings.HasPrefix(clientOrderId, "b-") {
		// buy order filled
		if orderId != m.grids[m.base+1].Order {
			log.Printf("[ERROR] buy order postion is NOT the base+1, base: %d", m.base)
			return
		}
		m.grids[m.base+1].Order = 0
		m.down()
	} else if strings.HasPrefix(clientOrderId, "s-") {
		// sell order filled
		if orderId != m.grids[m.base-1].Order {
			log.Printf("[ERROR] sell order postion is NOT the base+1, base: %d", m.base)
			return
		}
		m.grids[m.base-1].Order = 0
		m.up()
	} else {
		log.Printf("I don't know the clientOrderId: %s, maybe it's a manual order: %s", clientOrderId, orderId)
	}
}

func (m *Manager) buy(clientOrderId string, price, amount decimal.Decimal) (uint64, error) {
	log.Printf("[Order][buy] price: %s, amount: %s", price, amount)
	return m.ex.PlaceOrder(huobi.OrderTypeBuyLimit, m.symbol, clientOrderId, price, amount)
}

func (m *Manager) sell(clientOrderId string, price, amount decimal.Decimal) (uint64, error) {
	log.Printf("[Order][sell] price: %s, amount: %s", price, amount)
	return m.ex.PlaceOrder(huobi.OrderTypeSellLimit, m.symbol, clientOrderId, price, amount)
}

func (m *Manager) setupGridOrders(ctx context.Context) {
	for i := 0; i < len(m.grids); i++ {
		if i == m.base {
			continue
		} else if i < m.base {
			// sell
			clientOrderId := fmt.Sprintf("s-%d-%d", i, time.Now().Unix())
			orderId, err := m.sell(clientOrderId, m.grids[i].Price, m.grids[i].AmountSell)
			if err != nil {
				log.Printf("error when setupGridOrders, grid number: %d, err: %s", i, err)
				continue
			}
			m.grids[i].Order = orderId
		} else {
			// buy
			clientOrderId := fmt.Sprintf("b-%d-%d", i, time.Now().Unix())
			orderId, err := m.buy(clientOrderId, m.grids[i].Price, m.grids[i].AmountBuy)
			if err != nil {
				log.Printf("error when setupGridOrders, grid number: %d, err: %s", i, err)
				continue
			}
			m.grids[i].Order = orderId
		}
	}
}

func (m *Manager) cancelAllOrders(ctx context.Context) {
	for i := 0; i < len(m.grids); i++ {
		if m.grids[i].Order == 0 {
			continue
		}
		if ret, err := m.ex.CancelOrder(m.grids[i].Order); err == nil {
			log.Printf("cancel order successful, orderId: %d", m.grids[i].Order)
			m.grids[i].Order = 0

		} else {
			log.Printf("cancel order error: orderId: %d, return code: %d, err: %s", m.grids[i].Order, ret, err)
		}
	}
}

// 0: no need
// 1: buy
// -1: sell
// 2: no enough money
// -2: no enough coin
func (m *Manager) assetRebalancing(moneyNeed, coinNeed, moneyHeld, coinHeld, price decimal.Decimal) (direct int, amount decimal.Decimal) {
	if moneyNeed.Cmp(moneyHeld) == 1 {
		// sell coin
		moneyDelta := moneyNeed.Sub(moneyHeld)
		sellAmount := moneyDelta.Div(price).Round(m.amountPrecision)
		if coinHeld.Cmp(coinNeed.Add(sellAmount)) == -1 {
			log.Printf("no enough coin for rebalance: need hold %s and sell %s (%s in total), only have %s",
				coinNeed, sellAmount, coinNeed.Add(sellAmount), coinHeld)
			direct = -2
			return
		}

		if sellAmount.Cmp(m.minAmount) == -1 {
			log.Printf("sell amount %s less than minAmount(%s), won't sell", sellAmount, m.minAmount)
			direct = 0
			return
		}
		if m.minTotal.Cmp(price.Mul(sellAmount)) == 1 {
			log.Printf("sell total %s less than minTotal(%s), won't sell", price.Mul(sellAmount), m.minTotal)
			direct = 0
			return
		}
		direct = -1
		amount = sellAmount
	} else {
		// buy coin
		if coinNeed.Cmp(coinHeld) == -1 {
			log.Printf("no need to rebalance: need coin %s, has %s, need money %s, has %s",
				coinNeed, coinHeld, moneyNeed, moneyHeld)
			direct = 0
			return
		}
		coinDelta := coinNeed.Sub(coinHeld)
		buyTotal := coinDelta.Mul(price)
		if moneyHeld.Cmp(moneyNeed.Add(buyTotal)) == -1 {
			log.Fatalf("no enough money for rebalance: need hold %s and spend %s (%s in total)，only have %s",
				moneyNeed, buyTotal, moneyNeed.Add(buyTotal), moneyHeld)
			direct = 2
		}
		if coinDelta.Cmp(m.minAmount) == -1 {
			log.Printf("buy amount %s less than minAmount(%s), won't sell", coinDelta, m.minAmount)
			direct = 0
			return
		}
		if buyTotal.Cmp(m.minTotal) == -1 {
			log.Printf("buy total %s less than minTotal(%s), won't sell", buyTotal, m.minTotal)
			direct = 0
			return
		}
		direct = 1
		amount = coinDelta
	}
	return
}

func (m *Manager) up() {
	// make sure base >= 0
	if m.base == 0 {
		log.Println("grid base = 0, up OUT")
		return
	}
	m.base--
	if m.base < len(m.grids)-2 {
		// place buy order
		clientOrderId := fmt.Sprintf("b-%d-%d", m.base+1, time.Now().Unix())
		if orderId, err := m.buy(clientOrderId, m.grids[m.base+1].Price, m.grids[m.base+1].AmountBuy); err == nil {
			m.grids[m.base+1].Order = orderId
		}
	}
}

func (m *Manager) down() {
	// make sure base <= len(grids)
	if m.base == len(m.grids) {
		log.Printf("grid base = %d, down OUT", m.base)
		return
	}
	m.base++
	if m.base > 0 {
		// place sell order
		clientOrderId := fmt.Sprintf("s-%d-%d", m.base-1, time.Now().Unix())
		if orderId, err := m.sell(clientOrderId, m.grids[m.base-1].Price, m.grids[m.base-1].AmountSell); err == nil {
			m.grids[m.base-1].Order = orderId
		}
	}
}
