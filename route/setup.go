package route

import (
	"github.com/gin-gonic/gin"
)

func InitRoute() *gin.Engine {
	r := gin.Default()
	blockRoute(r)
	ShardRoute(r)
	// ForwardRoute(r)
	return r
}
