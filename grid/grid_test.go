package grid

import (
	"github.com/shopspring/decimal"
	"testing"
)

func TestManager_assetRebalancing(t *testing.T) {
	m := Trader{
		pricePrecision:  2,
		amountPrecision: 5,
		minAmount:       decimal.NewFromFloat(0.0001),
		minTotal:        decimal.NewFromInt(5),
	}
	price := decimal.NewFromInt(1)
	var tests = []struct {
		moneyNeed, coinNeed, moneyHeld, coinHeld float64
		direct                                   int
		amount                                   float64
	}{
		{100, 100, 200, 0, 1, 100},
		{100, 100, 0, 200, -1, 100},
		{100, 100, 100, 100, 0, 0},
		{100, 100, 200, 200, 0, 0},
		{100,100,0,0,-2,0},
	}
	for _, tt := range tests {
		amountExpect := decimal.NewFromFloat(tt.amount)
		direct, amount := m.assetRebalancing(decimal.NewFromFloat(tt.moneyNeed), decimal.NewFromFloat(tt.coinNeed),
			decimal.NewFromFloat(tt.moneyHeld), decimal.NewFromFloat(tt.coinHeld), price)
		if direct != tt.direct {
			t.Errorf("direct error, moneyNeed: %f, coinNeed: %f, moneyHeld: %f, coinHeld: %f, price: %s, direct expect: %d, actually: %d",
				tt.moneyNeed, tt.coinNeed, tt.moneyHeld, tt.coinHeld, price,
				direct, tt.direct)
		}
		if !amountExpect.Equal(amount) {
			t.Errorf("direct error, moneyNeed: %f, coinNeed: %f, moneyHeld: %f, coinHeld: %f, price: %s, amount expect: %d, actually: %d",
				tt.moneyNeed, tt.coinNeed, tt.moneyHeld, tt.coinHeld, price,
				amountExpect, amount)
		}
	}
}
