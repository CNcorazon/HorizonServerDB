package main

import (
	"io"
	"log"
	"os"
	"server/route"
	"server/structure"

	"github.com/gin-gonic/gin"
)

func main() {
	gin.DisableConsoleColor()
	f, _ := os.Create("gin.log")
	gin.DefaultWriter = io.MultiWriter(f)

	log.SetFlags(log.Lshortfile | log.LstdFlags)
	r := route.InitRoute()
	r.Run(structure.ServerPort)
}
