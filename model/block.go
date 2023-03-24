package model

import "server/structure"

type (
	BlockTransactionRequest struct {
		Shard uint
		Id    string
	}
	BlockTransactionResponse struct {
		Shard  uint
		Height uint
		Num    int
		// TxRoot         map[uint]string
		InternalList   map[uint][]structure.InternalTransaction
		CrossShardList map[uint][]structure.CrossShardTransaction
		RelayList      map[uint][]structure.SuperTransaction
		Signature      []structure.PubKeySign
	}

	BlockPackedTransactionResponse struct {
		Shard      uint
		Height     uint
		RootString map[uint]string
		Signsmap   map[uint][]structure.PubKeySign
	}

	BlockAccountRequest struct {
		Shard uint
	}

	BlockAccountResponse struct {
		Shard       uint
		Height      uint //当前区块的高度
		AccountList []structure.Account
		GSRoot      structure.GSRoot
	}

	//MessageType = 6
	BlockUploadRequest struct {
		// Shard     uint
		Id     string
		Height uint
		Block  structure.Block
		// ReLayList map[uint][]structure.SuperTransaction
	}

	BlockUploadResponse struct {
		// Shard   uint
		Height  uint
		Message string
	}

	TxWitnessRequest struct {
		Shard uint
	}

	TxWitnessResponse struct {
		// Id             string
		Shard          uint
		Height         uint
		Num            int
		InternalList   map[uint][]structure.InternalTransaction
		CrossShardList map[uint][]structure.CrossShardTransaction
		RelayList      map[uint][]structure.SuperTransaction
		Sign           structure.PubKeySign
	}

	//MessageType = 7
	TxWitnessRequest_2 struct {
		// Id             string
		Shard          uint
		Height         uint
		Num            int
		InternalList   map[uint][]structure.InternalTransaction
		CrossShardList map[uint][]structure.CrossShardTransaction
		RelayList      map[uint][]structure.SuperTransaction
		Sign           structure.PubKeySign
	}

	TxWitnessResponse_2 struct {
		Message string
		Flag    bool
	}

	//MessageType = 8
	RootUploadRequest struct {
		Shard  uint
		Height uint
		TxNum  int
		Id     string
		Root   string
		SuList map[uint][]structure.SuperTransaction
	}

	RootUploadResponse struct {
		// Shard   uint
		Height  uint
		Message string
	}
)
