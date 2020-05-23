package grid

import "github.com/shopspring/decimal"

type Grid struct {
	Id         int
	Price      decimal.Decimal
	AmountBuy  decimal.Decimal
	AmountSell decimal.Decimal
	TotalBuy   decimal.Decimal
	Order      uint64
}
