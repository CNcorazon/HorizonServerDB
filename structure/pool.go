package structure

import (
	"log"
	"server/logger"
)

//从witnessDB中打包需要处理的交易
//num1表示计划打包的内部交易的数目，num2表示需计划打包的跨分片交易数目，num3表示计划打包的接力交易数目
func PackTransactionList(height, shard, num1, num2 int) ([]InternalTransaction, []CrossShardTransaction, int) {
	translist := []Witnesstrans{}
	tx := WitnessTransDB.Begin()
	var InterTransList []InternalTransaction
	var CrossTransList []CrossShardTransaction
	//只读最前面的交易，模拟（可以加上offset=height*num1
	if err := tx.Where("shard = ? AND trans_type = 0", shard).Limit(num1).Find(&translist).Error; err != nil {
		log.Println(err)
		tx.Rollback()
		return []InternalTransaction{}, []CrossShardTransaction{}, 0
	} else {
		for _, rawtrans := range translist {
			tran := InternalTransaction{
				Shard: uint(rawtrans.Shard),
				From:  rawtrans.FromAddr,
				To:    rawtrans.ToAddr,
				Value: rawtrans.TransValue,
			}
			InterTransList = append(InterTransList, tran)
		}
	}

	if err := tx.Where("shard = ? AND trans_type=1", shard).Limit(num1).Find(&translist).Error; err != nil {
		tx.Rollback()
		return []InternalTransaction{}, []CrossShardTransaction{}, 0
	} else {
		for _, rawtrans := range translist {
			tran := CrossShardTransaction{
				Shard1: uint(rawtrans.Shard),
				Shard2: uint(rawtrans.ToShard),
				From:   rawtrans.FromAddr,
				To:     rawtrans.ToAddr,
				Value:  rawtrans.TransValue,
			}
			CrossTransList = append(CrossTransList, tran)
		}
	}

	tx.Commit()
	return InterTransList, CrossTransList, len(InterTransList) + len(CrossTransList)
}

// 打包witness超过一定阈值的交易
func PackWitnessedTransactionList(height, shard int) ([]InternalTransaction, []CrossShardTransaction, []SuperTransaction, int) {
	var Num int
	InternalList := []Witnesstrans{}
	CrossShardList := []Witnesstrans{}
	SuperList := []Witnesstrans{}
	var InterTransList []InternalTransaction
	var CrossTransList []CrossShardTransaction
	var SuperTransList []SuperTransaction

	tx := WitnessTransDB.Begin()

	if err := tx.Where("witness_num > ? AND trans_type = 0 AND shard = ?", CLIENT_MAX*2/3, shard).Limit(TX_NUM).Find(&InternalList).Error; err != nil {
		logger.AnalysisLogger.Printf("分片%v中没有witnessed内部交易！", shard)
		tx.Commit()
		return InterTransList, CrossTransList, SuperTransList, Num
	}
	for _, rawtrans := range InternalList {
		tran := InternalTransaction{
			Shard: uint(rawtrans.Shard),
			From:  rawtrans.FromAddr,
			To:    rawtrans.ToAddr,
			Value: rawtrans.TransValue,
		}
		InterTransList = append(InterTransList, tran)
	}

	if err := tx.Where("witness_num > ? AND trans_type = 1 AND shard = ?", CLIENT_MAX*2/3, shard).Limit(TX_NUM).Find(&CrossShardList).Error; err != nil {
		logger.AnalysisLogger.Printf("分片%v中没有witnessed跨分片交易！", shard)
		tx.Commit()
		return InterTransList, CrossTransList, SuperTransList, Num
	}
	for _, rawtrans := range CrossShardList {
		tran := CrossShardTransaction{
			Shard1: uint(rawtrans.Shard),
			Shard2: uint(rawtrans.ToShard),
			From:   rawtrans.FromAddr,
			To:     rawtrans.ToAddr,
			Value:  rawtrans.TransValue,
		}
		CrossTransList = append(CrossTransList, tran)
	}

	if err := tx.Where("witness_num > ? AND trans_type = 3 AND shard = ?", CLIENT_MAX*2/3, shard).Limit(TX_NUM).Find(&SuperList).Error; err != nil {
		logger.AnalysisLogger.Printf("分片%v中没有超级交易！", shard)
		tx.Commit()
		return InterTransList, CrossTransList, SuperTransList, Num
	}
	for _, rawtrans := range SuperList {
		tran := SuperTransaction{
			Shard: uint(rawtrans.Shard),
			To:    rawtrans.ToAddr,
			Value: rawtrans.TransValue,
		}
		SuperTransList = append(SuperTransList, tran)
	}
	tx.Commit()
	Num += len(InterTransList) + len(CrossTransList) + len(SuperTransList)
	return InterTransList, CrossTransList, SuperTransList, Num
}

func PackValidTransactionList(height, shard int) ([]InternalTransaction, []CrossShardTransaction, []SuperTransaction, int, int, int) {
	translist := []Witnesstrans{}
	tx := WitnessTransDB.Begin()
	var InterTransList []InternalTransaction
	var CrossTransList []CrossShardTransaction
	var SuperTransList []SuperTransaction
	//只读最前面的交易，模拟（可以加上offset=height*num1
	if err := tx.Where("shard = ? AND trans_type=0", shard).Limit(TX_NUM).Find(&translist).Error; err != nil {
		tx.Rollback()
		return []InternalTransaction{}, []CrossShardTransaction{}, []SuperTransaction{}, 0, 0, 0
	} else {
		for _, rawtrans := range translist {
			tran := InternalTransaction{
				Shard: uint(rawtrans.Shard),
				From:  rawtrans.FromAddr,
				To:    rawtrans.ToAddr,
				Value: rawtrans.TransValue,
			}
			InterTransList = append(InterTransList, tran)
		}
	}

	if err := tx.Where("shard = ? AND trans_type=1", shard).Limit(TX_NUM).Find(&translist).Error; err != nil {
		tx.Rollback()
		return []InternalTransaction{}, []CrossShardTransaction{}, []SuperTransaction{}, 0, 0, 0
	} else {
		for _, rawtrans := range translist {
			tran := CrossShardTransaction{
				Shard1: uint(rawtrans.Shard),
				Shard2: uint(rawtrans.ToShard),
				From:   rawtrans.FromAddr,
				To:     rawtrans.ToAddr,
				Value:  rawtrans.TransValue,
			}
			CrossTransList = append(CrossTransList, tran)
		}
	}

	if err := tx.Where("shard = ? AND trans_type=2", shard).Limit(TX_NUM).Find(&translist).Error; err != nil {
		tx.Rollback()
		return []InternalTransaction{}, []CrossShardTransaction{}, []SuperTransaction{}, 0, 0, 0
	} else {
		for _, rawtrans := range translist {
			tran := SuperTransaction{
				Shard: uint(rawtrans.Shard),
				To:    rawtrans.ToAddr,
				Value: rawtrans.TransValue,
			}
			SuperTransList = append(SuperTransList, tran)
		}
	}

	tx.Commit()
	return InterTransList, CrossTransList, SuperTransList, len(InterTransList), len(CrossTransList), len(SuperTransList)
}

func AppendTransaction(trans Witnesstrans) error {
	//写进数据库
	tx := WitnessTransDB.Begin()
	if err := tx.Create(&trans).Error; err != nil {
		tx.Rollback()
		return err
	}
	tx.Commit()
	return nil
}
