module github.com/xyths/grid-trading

go 1.14

require (
	github.com/huobirdcenter/huobi_golang v0.0.0-00010101000000-000000000000
	github.com/shopspring/decimal v1.2.0
	github.com/urfave/cli/v2 v2.2.0
	github.com/xyths/hs v0.0.0-00010101000000-000000000000
	go.mongodb.org/mongo-driver v1.3.2
)

replace github.com/xyths/hs => ../hs

replace github.com/huobirdcenter/huobi_golang => ../../huobirdcenter/huobi_Golang
