package controller

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"log"
	"net/http"
	"server/logger"
	"server/model"
	"server/structure"
	"time"

	"github.com/gin-gonic/gin"
)

// type (
// 	shardTx struct {
// 		intTx []structure.InternalTransaction
// 		croTx []structure.CrossShardTransaction
// 		reTx  []structure.SuperTransaction
// 	}
// )

func PackTransaction(c *gin.Context) {
	var data model.BlockTransactionRequest
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	shard := data.Shard
	height := structure.GetHeight()
	IntList := make(map[uint][]structure.InternalTransaction)
	CroList := make(map[uint][]structure.CrossShardTransaction)
	var Num int

	// 这边为打包witnessedtrans，生成唯一的proposal直接去共识
	// 前一个版本比较详细，做法为区块见证阶段各个proposer生成各自的proposal，这一部分为打包proposals，交给winner去检查: 大家都能下载到同一份txlist
	if shard == 0 {
		RootString := make(map[uint]string)
		for i := 1; i <= structure.ShardNum; i++ {
			IntList = make(map[uint][]structure.InternalTransaction)
			CroList = make(map[uint][]structure.CrossShardTransaction)
			ReList := make(map[uint][]structure.SuperTransaction)

			Int, Cro, Re, num := structure.PackWitnessedTransactionList(int(height)+1, i)
			IntList[uint(i)] = append(IntList[uint(i)], Int...)
			CroList[uint(i)] = append(CroList[uint(i)], Cro...)
			ReList[uint(i)] = append(ReList[uint(i)], Re...)
			Num = Num + num

			list2marshal := structure.TransactionBlock{
				Height:         uint(height) + 1,
				InternalList:   IntList,
				CrossShardList: CroList,
				SuperList:      ReList,
			}
			jsonString, _ := json.Marshal(list2marshal)
			hash := sha256.Sum256(jsonString)
			s := hex.EncodeToString(hash[:])
			RootString[uint(i)] = s
		}
		logger.AnalysisLogger.Printf("共识节点%v打包了%v条交易", data.Id, Num)

		res := model.BlockPackedTransactionResponse{
			Shard:      shard,
			Height:     uint(structure.GetHeight()) + 1,
			RootString: RootString,
			// Signsmap:   Signs,
		}
		c.JSON(200, res)
	} else {
		// 区块见证打包交易
		Int, Cro, num := structure.PackTransactionList(int(height)+1, int(shard), structure.TX_NUM, structure.TX_NUM)
		IntList[uint(shard)] = append(IntList[uint(shard)], Int...)
		CroList[uint(shard)] = append(CroList[uint(shard)], Cro...)
		Num = Num + num

		logger.AnalysisLogger.Printf("分片%v中节点%v见证了%v条交易", shard, data.Id, Num)
		res := model.BlockTransactionResponse{
			Shard:          shard,
			Height:         uint(structure.GetHeight() + 2),
			Num:            Num,
			InternalList:   IntList,
			CrossShardList: CroList,
		}
		c.JSON(200, res)
	}
}

// 共识分片中的节点通过该函数，请求将共识区块（即交易列表）上链
func AppendBlock(c *gin.Context) {
	// time1 := time.Now()
	var data model.BlockUploadRequest
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	//检测这个ChainBlock是否合理，如果合理就Append到链上
	err1 := structure.Source.ChainShard[0].VerifyBlock(data.Block)
	if err1 != nil {
		log.Println(err1)
		return
	} else {
		height := structure.GetHeight()
		IntList := make(map[uint][]structure.InternalTransaction)
		CroList := make(map[uint][]structure.CrossShardTransaction)
		ReList := make(map[uint][]structure.SuperTransaction)
		var Num int
		for i := 1; i <= structure.ShardNum; i++ {
			Int, Cro, Re, num := structure.PackWitnessedTransactionList(int(height)+1, i)
			IntList[uint(i)] = append(IntList[uint(i)], Int...)
			CroList[uint(i)] = append(CroList[uint(i)], Cro...)
			ReList[uint(i)] = append(ReList[uint(i)], Re...)
			Num = Num + num
		}
		transaction2updata := structure.TransactionBlock{
			InternalList:   IntList,
			CrossShardList: CroList,
			SuperList:      ReList, //需要被打包进这个区块内部的SuperList
		}
		logger.AnalysisLogger.Printf("收到的block:%v", data.Block)
		// append到chainDB中, 并且更新GSroot
		structure.Source.ChainShard[0].AppendBlock(data.Block)
		// 更新内存中的账户状态
		structure.UpdateChain(transaction2updata, data.Block.Header.Height, structure.Source.ChainShard[0].AccountState)
		logger.BlockLogger.Printf("添加区块成功, 该区块获得了%v张投票, 当前的区块高度是%v,当前的时间是 %v\n", data.Block.Header.Vote, height+1, time.Now().UnixMicro())
	}
	for i := 1; i <= structure.ShardNum; i++ {
		structure.Source.ChainShard[uint(0)].AccountState.RootsVote[uint(i)] = structure.Source.ChainShard[uint(0)].AccountState.NewRootsVote[uint(i)]
		structure.Source.ChainShard[uint(0)].AccountState.NewRootsVote[uint(i)] = make(map[string]int)
		structure.Source.ChainShard[uint(0)].NodeNum = structure.NewNodeNum()
		structure.Source.ClientShard[uint(i)].AccountState.Tx_num = 0
		structure.Source.ClientShard[uint(i)].AccountState.NewRootsVote = make(map[uint]map[string]int)
		structure.Source.ClientShard[uint(i)].ValidEnd = false
		for j := 1; j <= structure.ShardNum; j++ {
			structure.Source.ClientShard[uint(i)].AccountState.NewRootsVote[uint(j)] = make(map[string]int)
		}
	}
	// logger.AnalysisLogger.Println("开始重分片，清空clientDB")
	// tx := structure.ClientDB.Begin()
	// if err := tx.Set("gorm:query_option", "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE").Error; err != nil {
	// 	tx.Rollback()
	// 	return
	// }
	// if err := tx.Exec("DELETE FROM clients").Error; err != nil {
	// 	tx.Rollback()
	// 	logger.AnalysisLogger.Println("删除失败")
	// }
	// if err := tx.Exec("ALTER TABLE clients AUTO_INCREMENT=0;").Error; err != nil {
	// 	tx.Rollback()
	// 	logger.AnalysisLogger.Println("重置自增失败")
	// }
	// if err := tx.Commit().Error; err != nil {
	// 	tx.Rollback()
	// 	logger.AnalysisLogger.Println("提交失败")
	// }
	// tx.Commit()
	// logger.AnalysisLogger.Println("重置完成")

	// 将共识分片中的客户端全部关掉，concurrentmap中不知道怎么删除，不删了
	// for _, value := range structure.Source.ClientShard[0].Consensus_CommunicationMap {
	// 	if value.Socket != nil {
	// 		value.Socket.Close()
	// 	}
	// }

	res := model.BlockUploadResponse{
		Height:  uint(structure.GetHeight()),
		Message: "添加共识区块成功",
	}
	c.JSON(200, res)
}

func WitnessTx(c *gin.Context) {
	var data model.TxWitnessRequest_2
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// shard := data.Shard

	//更新witnesstransDB中的witnessnum
	tx := structure.WitnessTransDB.Begin()
	txlist := []structure.Witnesstrans{}
	if err := tx.Where("id<?", 2*structure.TX_NUM+1).Find(&txlist).Error; err != nil {
		tx.Rollback()
		return
	}
	tx_temp := structure.Witnesstrans{
		WitnessNum: txlist[0].WitnessNum + 1,
	}
	if err := tx.Model(&txlist).Updates(tx_temp).Error; err != nil {
		tx.Rollback()
		return
	}
	tx.Commit()

	logger.AnalysisLogger.Printf("见证成功%v条交易", len(txlist))

	res := model.TxWitnessResponse_2{
		Message: "见证成功!",
		Flag:    true,
	}

	c.JSON(200, res)
}

// 账户应该搞个数据库
func PackAccount(c *gin.Context) {
	var data model.BlockAccountRequest
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	shard := data.Shard
	// structure.Source.ChainShard[0].Lock.Lock()
	accountList := structure.Source.ChainShard[0].AccountState.GetAccountList()
	gsroot := structure.GSRoot{
		StateRoot: structure.Source.ChainShard[0].AccountState.CalculateRoot(),
		Vote:      structure.Source.ChainShard[0].AccountState.RootsVote,
	}
	// logger.AnalysisLogger.Printf("来自分片%v的gsroot请求,此时rootsvote为:%v", shard, structure.Source.ChainShard[0].AccountState.RootsVote)
	height := structure.GetHeight()
	// structure.Source.ChainShard[0].Lock.Unlock()

	res := model.BlockAccountResponse{
		Shard:       shard,
		Height:      uint(height),
		AccountList: accountList,
		GSRoot:      gsroot,
	}
	// logger.AnalysisLogger.Println(res)
	c.JSON(200, res)
}

func PackValidTx(c *gin.Context) {
	var data model.BlockTransactionRequest
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	shard := data.Shard

	height := structure.GetHeight() - 1
	IntList := make(map[uint][]structure.InternalTransaction)
	CroList := make(map[uint][]structure.CrossShardTransaction)
	ReList := make(map[uint][]structure.SuperTransaction)
	var Num int

	Int, Cro, Sup, num1, num2, num3 := structure.PackValidTransactionList(int(height)+1, int(shard))
	IntList[uint(shard)] = append(IntList[uint(shard)], Int...)
	CroList[uint(shard)] = append(CroList[uint(shard)], Cro...)
	ReList[uint(shard)] = append(ReList[uint(shard)], Sup...)

	logger.AnalysisLogger.Printf("执行节点验证阶段打包了%v,%v,%v条交易", num1, num2, num3)
	Num = num1 + num2 + num3
	res := model.BlockTransactionResponse{
		Shard:          shard,
		Height:         uint(structure.GetHeight() + 1),
		Num:            Num,
		InternalList:   IntList,
		CrossShardList: CroList,
		RelayList:      ReList,
	}

	c.JSON(200, res)
}

func CollectRoot(c *gin.Context) {
	var data model.RootUploadRequest
	//判断请求的结构体是否符合定义
	if err := c.ShouldBindJSON(&data); err != nil {
		// gin.H封装了生成json数据的工具
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	height := data.Height
	tx_num := data.TxNum
	root := data.Root
	shard := data.Shard
	sulist := data.SuList
	structure.Source.ClientShard[shard].Lock.Lock()

	structure.Source.ClientShard[uint(shard)].AccountState.NewRootsVote[shard][root] += 1
	logger.AnalysisLogger.Printf("收到来自shard%v的树根投票,该分片的树根以及票数情况为:votes%v", shard, structure.Source.ClientShard[uint(shard)].AccountState.NewRootsVote[shard])

	isEnd := structure.Source.ClientShard[shard].ValidEnd
	flag := false
	for _, num := range structure.Source.ClientShard[uint(shard)].AccountState.NewRootsVote[shard] {
		if num >= 1 {
			flag = true
		}
	}
	if !isEnd && flag {
		for j := 1; j <= structure.ShardNum; j++ {
			for _, tran := range sulist[uint(j)] {
				traninDB := structure.Witnesstrans{
					Shard:      int(tran.Shard),
					ToAddr:     tran.To,
					TransValue: tran.Value,
				}
				structure.AppendTransaction(traninDB)
			}
		}
		structure.Source.ClientShard[shard].AccountState.Tx_num += tx_num
		logger.BlockLogger.Printf("分片%v验证成功%v条交易\n", shard, tx_num)
		structure.Source.ClientShard[shard].ValidEnd = true
	}

	structure.Source.ClientShard[shard].Lock.Unlock()
	res := model.RootUploadResponse{
		Height:  height,
		Message: "树根上传成功",
	}
	c.JSON(200, res)

}
