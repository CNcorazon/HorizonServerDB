package route

import (
	"server/controller"

	"github.com/gin-gonic/gin"
)

func ShardRoute(r *gin.Engine) {
	group := r.Group("/shard")
	{
		group.GET("/shardNum", controller.PreRegister)
		group.GET("/height", controller.GetHeight)
		group.GET("/register/:random", controller.RegisterCommunication)
		group.POST("/block", controller.MultiCastBlock)
		group.POST("/vote", controller.SendVote)
	}
}
