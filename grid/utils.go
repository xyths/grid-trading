package grid

import (
	"fmt"
	"time"
)

func getClientOrderId(prefix string) string {
	return fmt.Sprintf("%s-%d", prefix, time.Now().Unix())
}
