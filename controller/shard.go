package controller

import (
	"encoding/json"
	"log"
	"math"
	"net/http"
	"server/logger"
	"server/model"
	"server/structure"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	uuid "github.com/satori/go.uuid"
	"gorm.io/gorm"
)

// const (
// 	WsRequest     = "/forward/wsRequest"
// 	ClientForward = "/forward/clientRegister "
// )

// client登录进数据库（节点预登记）
func PreRegister(c *gin.Context) {
	var BlockHeight int64
	// 读取区块高度
	if err := structure.ClientDB.Exec("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE").Error; err != nil {
		log.Printf("设置隔离级别失败: %v", err)
		return
	}
	tx2 := structure.ChainDB.Begin()
	// if err := tx2.Set("gorm:query_option", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").Error; err != nil {
	// 	tx2.Rollback()
	// 	logger.AnalysisLogger.Printf("设置事务隔离级别失败")
	// 	return
	// }
	if err := tx2.Model(&structure.Blocks{}).Count(&BlockHeight).Error; err != nil {
		return
	}
	tx2.Commit()
	// 写入clientDB
	tx1 := structure.ClientDB.Begin()
	client := structure.Clients{
		Shard:      -1,
		Height:     int(BlockHeight) + 1,
		ClientType: 0,
	}
	if err := tx1.Create(&client).Error; err != nil {
		tx1.Rollback()
		logger.AnalysisLogger.Printf("插入失败")
		return
	}

	if err := tx1.Commit().Error; err != nil {
		tx1.Rollback()
		logger.AnalysisLogger.Printf("提交失败")
	}
	res := model.ShardNumResponse{
		ShardNum: uint(12138),
	}
	c.JSON(200, res)
}

func RegisterCommunication(c *gin.Context) {
	random, _ := strconv.Atoi(c.Param("random"))

	if err := structure.ClientDB.Exec("SET SESSION TRANSACTION ISOLATION LEVEL SERIALIZABLE").Error; err != nil {
		log.Printf("设置隔离级别失败: %v", err)
		return
	}
	h := structure.GetHeight() + 1

	// shardnum := 0
	// NodesNumMap := structure.Source.ChainShard[0].NodeNum
	// NodesNumMap.Mu.Lock()
	// for shardnum1, nodenum := range NodesNumMap.Nodeshard {
	// 	if nodenum < structure.ProposerNum/structure.ShardNum {
	// 		shardnum = shardnum1
	// 		NodesNumMap.Nodeshard[shardnum]++
	// 		break
	// 	}
	// }
	// if shardnum == 0 {
	// 	log.Printf("已满")
	// 	NodesNumMap.Mu.Unlock()
	// 	return
	// }
	// log.Println(NodesNumMap.Nodeshard)
	// NodesNumMap.Mu.Unlock()
	SHARDNUM := 0
	var ID string
	maxRetries := 3
	for i := 0; i < maxRetries; i++ {
		err := structure.ClientDB.Transaction(func(tx *gorm.DB) error {
			// 在事务中执行数据库操作
			clientinDB := structure.Clients{}
			result0 := tx.Where("shard = -1 AND height = ?", h).Limit(1).Find(&clientinDB)
			shardnum := int(math.Ceil(float64(clientinDB.Id%structure.ProposerNum) / (float64(structure.ProposerNum) / float64(structure.ShardNum))))
			if shardnum == 0 {
				shardnum = structure.ShardNum
			}
			logger.ShardLogger.Printf("该共识节点被分配到了第%v个区块的第%v个分片", h, shardnum)
			id := uuid.NewV4().String()
			result1 := tx.Model(&clientinDB).Where("shard = -1 AND height = ?", h).Updates(structure.Clients{Shard: shardnum, Pubkey: id, ClientType: 1, Random: random, Height: h})

			ID = id
			SHARDNUM = shardnum

			if result0.Error != nil {

				return result0.Error
			}
			if result1.Error != nil {
				return result1.Error
			}

			// 如果操作成功，返回 nil，事务将会被提交
			return nil
		})
		if err == nil {
			// log.Println()
			break
		}
		if strings.Contains(err.Error(), "Deadlock found") && i < maxRetries-1 {
			log.Printf("死锁检测到，重试事务（尝试 %d/%d）", i+1, maxRetries)
		} else {
			log.Printf("事务失败: %v,重试事务，尝试 %d/%d）", err, i+1, maxRetries)
			// break
		}
		time.Sleep(50 * time.Millisecond)
	}
	//将http请求升级成为WebSocket请求
	upGrader := websocket.Upgrader{
		// cross origin domain
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
		// 处理 Sec-WebSocket-Protocol Header
		Subprotocols: []string{c.GetHeader("Sec-WebSocket-Protocol")},
	}
	conn, err := upGrader.Upgrade(c.Writer, c.Request, nil)
	if err != nil {
		log.Printf("websocket connect error: %s", c.Param("channel"))
		return
	}
	client := &structure.MapClient{
		Id:     ID,
		Shard:  uint(SHARDNUM),
		Socket: conn,
		Random: random,
	}
	Consensus_Map := structure.Source.ClientShard[uint(SHARDNUM)].Consensus_CommunicationMap
	Consensus_Map.Put(client.Id, client)

	clients := []structure.Clients{}
	tx := structure.ClientDB.Begin()
	tx.Where("shard = ? AND client_type = 1 AND height = ? ", SHARDNUM, h).Find(&clients)
	logger.ShardLogger.Printf("分片%v中有%v个节点", SHARDNUM, len(clients))
	tx.Commit()

	if len(clients) == structure.ProposerNum/structure.ShardNum {
		logger.ShardLogger.Printf("执行分片%v切换到了生成委员会阶段", SHARDNUM)
		//根据Client提交的Random筛选出本执行分片中的proposers，进入委员会
		//proposer的个数为20，每个分片就是20/shardnum，当shardnum=1,2,4,5,10,20时，对应于选择20,10,5,4,2,1个proposer
		// var CommonClients []structure.Clients
		vrf := make([]int, 0)
		ProposerClientId := make([]string, 0)

		for _, client := range clients {
			vrf = append(vrf, client.Random)
		}
		logger.ShardLogger.Println(vrf)
		sort.Ints(sort.IntSlice(vrf))
		for _, element := range vrf {
			for _, value := range clients {
				if value.Random == element {
					// log.Println(value.Random, element)
					ProposerClientId = append(ProposerClientId, value.Pubkey)
				}
			}
		}
		//proposers记录进分片0，即委员会

		for _, clientid := range ProposerClientId {
			// log.Println(clientid)
			v, err := Consensus_Map.Get(clientid, 0)
			if err != nil {
				logger.ShardLogger.Printf("client%v不存在", clientid)
				return
			}
			structure.Source.ClientShard[0].Consensus_CommunicationMap.Put(clientid, v)
		}

		tx4 := structure.ClientDB.Begin()
		proposerclients := []structure.Clients{}
		if err := tx4.Where("client_type = 1 AND height = ?", h).Find(&proposerclients).Error; err != nil {
			log.Printf("查找失败")
			tx4.Rollback()
			return
		}
		tx4.Commit()

		//判断共识委员会是本区块的还是下一个区块的
		// if structure.GetHeight() == 0 {
		// 	structure.Source.ClientShard[uint(shardnum)].Validation_CommunicationMap = Consensus_Map
		// } else {
		// }

		// logger.AnalysisLogger.Printf("分片%v填满节点后consensus_map中的节点有:%v", shardnum, structure.Source.ClientShard[uint(shardnum)].Consensus_CommunicationMap)
		// logger.AnalysisLogger.Printf("分片%v填满节点后proposer_consensus_map中的节点:%v", shardnum, structure.Source.ClientShard[uint(0)].Consensus_CommunicationMap)

		//若所有执行分片都选出了胜者，则在委员会中选出最终胜者
		if len(proposerclients) == structure.ProposerNum {
			logger.ShardLogger.Printf("所有执行分片都选出了胜者，开始进行共识")
			// for i := 1; i <= structure.ShardNum; i++ {
			// 	structure.Source.ClientShard[uint(i)].New_Validation_CommunicationMap = structure.Source.ClientShard[uint(i)].Consensus_CommunicationMap
			// }

			var FinalWin string
			FinalRandom := structure.MAX
			var idlist []string

			for _, clientinDB := range proposerclients {
				client, err := structure.Source.ClientShard[0].Consensus_CommunicationMap.Get(clientinDB.Pubkey, 0)
				if err != nil {
					logger.AnalysisLogger.Printf("找不到client:%v", clientinDB.Pubkey)
					return
				}
				if client.Random <= FinalRandom {
					FinalWin = client.Id
					FinalRandom = client.Random
				}
				idlist = append(idlist, client.Id)
			}

			//通知共识节点各自的身份
			for _, proposerclientinDB := range proposerclients {
				client, err := structure.Source.ClientShard[0].Consensus_CommunicationMap.Get(proposerclientinDB.Pubkey, 0)
				if err != nil {
					return
				}
				if client.Id == FinalWin {
					// logger.ShardLogger.Printf("节点%v是获胜者")
					message := model.MessageIsWin{
						IsWin:       true,
						IsConsensus: true,
						WinID:       FinalWin,
						PersonalID:  FinalWin,
						IdList:      idlist,
						ShardNum:    int(client.Shard),
					}
					payload, err := json.Marshal(message)
					if err != nil {
						log.Println(err)
						return
					}
					metamessage := model.MessageMetaData{
						MessageType: 1,
						Message:     payload,
					}
					client.Socket.WriteJSON(metamessage)
				} else {
					message := model.MessageIsWin{
						IsWin:       false,
						IsConsensus: true,
						WinID:       FinalWin,
						PersonalID:  client.Id,
						IdList:      idlist,
						ShardNum:    int(client.Shard),
					}
					payload, err := json.Marshal(message)
					if err != nil {
						log.Println(err)
						return
					}
					metamessage := model.MessageMetaData{
						MessageType: 1,
						Message:     payload,
					}
					client.Socket.WriteJSON(metamessage)
				}
			}
		}
	}
}

func GetHeight(c *gin.Context) {
	res := model.HeightResponse{
		Height: structure.GetHeight(),
	}
	c.JSON(200, res)
}

//共识分片内部的胜利者计算出区块之后，使用该函数向分片内部的节点转发计算出来得到的区块
func MultiCastBlock(c *gin.Context) {
	var data model.MultiCastBlockRequest
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// payload, err := json.Marshal(data)
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }
	// logger.TimestampLogger.Printf("广播的区块大小为%v", unsafe.Sizeof(payload))
	// metaMessage := model.MessageMetaData{
	// 	MessageType: 2,
	// 	Message:     payload,
	// }

	// var CommunicationMap map[uint]map[string]*structure.Client
	// if len(structure.Source.CommunicationMap[uint(0)]) > len(structure.Source.CommunicationMap_temp[uint(0)]) {
	// 	CommunicationMap = structure.Source.CommunicationMap
	// } else {
	// 	CommunicationMap = structure.Source.CommunicationMap_temp
	// }

	//向连接在该服务器的委员会成员转发数据，注意不用向自己转发

	// for key, value := range structure.Source.ClientShard[0].Consensus_CommunicationMap {
	// 	if key != data.Id && value.Socket != nil {
	// 		logger.AnalysisLogger.Printf("将区块发送给委员会成员")
	// 		value.Socket.WriteJSON(metaMessage)
	// 	}
	// }

	res := model.MultiCastBlockResponse{
		Message: "Group multicast block succeed",
	}
	c.JSON(200, res)
}

func SendVote(c *gin.Context) {
	// start := time.Now()
	var data model.SendVoteRequest
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// // structure.Source.ClientShard[0].NodeNum = structure.Source.ClientShard[0].NodeNum + 1

	// // shardnum := data.Shard
	// target := data.WinID
	// // log.Println(target)
	// // log.Println(shardnum)
	// payload, err := json.Marshal(data)
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }
	// metaMessage := model.MessageMetaData{
	// 	MessageType: 3,
	// 	Message:     payload,
	// }
	// structure.Source.ClientShard[data.Shard].Lock.Lock()
	// // structure.Source.ClientShard[0].Lock.Lock()
	// //logger.AnalysisLogger.Println(structure.Source.ClientShard[data.Shard].MultiCastConn)

	// if structure.Source.ClientShard[data.Shard].MultiCastConn != nil {
	// 	//logger.AnalysisLogger.Println(structure.Source.ClientShard[0].Consensus_CommunicationMap[target])
	// 	if structure.Source.ClientShard[0].Consensus_CommunicationMap[target].Socket != nil {
	// 		structure.Source.ClientShard[data.Shard].MultiCastConn.WriteJSON(metaMessage)
	// 	} else {
	// 		for _, value := range structure.Source.Write_Server_CommunicationMap {
	// 			value.Lock.Lock()
	// 			value.Socket.WriteJSON(metaMessage)
	// 			value.Lock.Unlock()
	// 		}
	// 	}
	// }
	// // time := time.Since(start)
	// // logger.AnalysisLogger.Println(time)
	// // structure.Source.ClientShard[data.Shard].Lock.Unlock()
	// structure.Source.ClientShard[0].Lock.Unlock()
	// // }

	res := model.SendVoteResponse{
		Message: "Group multicast Vote succeed",
	}
	c.JSON(200, res)
}

// func MuiltiCastCommunication(c *gin.Context) {
// 	shardnum, _ := strconv.Atoi(c.Param("shardnum"))

// 	//将http请求升级成为WebSocket请求
// 	upGrader := websocket.Upgrader{
// 		// cross origin domain
// 		CheckOrigin: func(r *http.Request) bool {
// 			return true
// 		},
// 		// 处理 Sec-WebSocket-Protocol Header
// 		Subprotocols: []string{c.GetHeader("Sec-WebSocket-Protocol")},
// 	}

// 	conn, err := upGrader.Upgrade(c.Writer, c.Request, nil)
// 	if err != nil {
// 		log.Printf("websocket connect error: %s", c.Param("channel"))
// 		return
// 	}

// 	structure.Source.ClientShard[uint(shardnum)].MultiCastConn = conn
// 	//转发给其他服务器
// }

// func stringInSlice(a string, list []string) bool {
// 	for _, b := range list {
// 		if b == a {
// 			return true
// 		}
// 	}
// 	return false
// }

// clientinDB.Shard = shardnum
// clientinDB.Pubkey = client.Id
// clientinDB.ClientType = 1
// clientinDB.Random = client.Random
// clientinDB.Height = h

// func updateClient(db *gorm.DB, clientinDB *structure.Clients, shardnum int, Pubkey string, client_type int, random int, h int) error {
// 	// 重试次数
// 	retryCount := 3
// 	var err error

// 	for i := 0; i < retryCount; i++ {
// 		err = db.Transaction(func(tx *gorm.DB) error {
// 			// 更新用户
// 			clientinDB.Shard = shardnum
// 			clientinDB.Pubkey = Pubkey
// 			clientinDB.ClientType = 1
// 			clientinDB.Random = random
// 			clientinDB.Height = h
// 			result := tx.Save(clientinDB)
// 			if result.Error != nil {
// 				return result.Error
// 			}

// 			return nil
// 		})

// 		// 如果事务成功，跳出循环
// 		if err == nil {
// 			break
// 		}
// 	}
// 	// 等待一段时间再次尝试
// 	time.Sleep(50 * time.Millisecond)

// 	return err
// }
