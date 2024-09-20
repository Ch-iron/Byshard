package pbft

import (
	"sync"
	"unishard/crypto"
	"unishard/log"
	"unishard/message"
	"unishard/quorum"
	"unishard/types"
	"unishard/utils"

	"github.com/ethereum/go-ethereum/common"
)

type decideMajority struct {
	Hash            common.Hash
	TotalVote       int
	True            int
	False           int
	IsCollectAll    chan bool
	increasetruemu  sync.Mutex
	increasefalsemu sync.Mutex
}

func (pb *PBFT) AddDecidingTransaction(hash common.Hash, length int) *decideMajority {
	pb.majoritymu.Lock()
	defer pb.majoritymu.Unlock()
	if pb.transactionQuorum[hash] != nil {
		if pb.transactionQuorum[hash].TotalVote == 0 {
			pb.transactionQuorum[hash].TotalVote = length
		}
		return pb.transactionQuorum[hash]
	}
	pb.transactionQuorum[hash] = &decideMajority{
		Hash:            hash,
		TotalVote:       length,
		True:            0,
		False:           0,
		IsCollectAll:    make(chan bool, 2),
		increasetruemu:  sync.Mutex{},
		increasefalsemu: sync.Mutex{},
	}
	return pb.transactionQuorum[hash]
}

func (pb *PBFT) GetDecidingTransaction(hash common.Hash) *decideMajority {
	pb.majoritymu.RLock()
	defer pb.majoritymu.RUnlock()
	return pb.transactionQuorum[hash]
}

func (pb *PBFT) DeleteDecidingTransaction(hash common.Hash) {
	pb.majoritymu.Lock()
	defer pb.majoritymu.Unlock()
	delete(pb.transactionQuorum, hash)
}

func (pb *PBFT) SetAbortTransaction(tx *decideMajority) {
	tx.increasefalsemu.Lock()
	defer tx.increasefalsemu.Unlock()
	tx.False++
	if !pb.IsLeader(pb.ID(), pb.pm.GetCurView(), pb.pm.GetCurEpoch()) {
		if tx.TotalVote == tx.True+tx.False {
			log.DecideDebugf("[%v %v] (SetAbortTransaction) total vote is same!!! Hash: %v", pb.ID(), pb.Shard(), tx.Hash)
			tx.IsCollectAll <- true
		}
	}
}

func (pb *PBFT) IncreaseVote(tx *decideMajority) {
	tx.increasetruemu.Lock()
	defer tx.increasetruemu.Unlock()
	tx.True++
	if !pb.IsLeader(pb.ID(), pb.pm.GetCurView(), pb.pm.GetCurEpoch()) {
		if tx.TotalVote == tx.True+tx.False {
			log.DecideDebugf("[%v %v] (IncreaseVote) total vote is same!!! Hash: %v", pb.ID(), pb.Shard(), tx.Hash)
			tx.IsCollectAll <- true
		}
	}
}

func (pb *PBFT) IsDecide(tx *decideMajority) bool {
	if tx.TotalVote == 0 {
		return false
	}
	if tx.TotalVote == tx.True+tx.False {
		return true
	} else {
		return false
	}
}

func (pb *PBFT) IsAbort(tx *decideMajority) bool {
	if tx.False > 0 {
		return true
	} else {
		return false
	}
}

func (pb *PBFT) InitDecideStep(tx *message.VotedTransactionWithHeader) *message.VotedTransactionWithHeader {
	log.DecideDebugf("[%v %v] (InitDecideStep) Init Decide Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), pb.pm.GetCurView(), pb.pm.GetCurEpoch(), tx.Transaction.Transaction.Hash)

	decideMajorityTransaction := pb.GetDecidingTransaction(tx.Transaction.Hash)
	if decideMajorityTransaction == nil {
		var associate_shards []types.Shard
		associate_addresses := []common.Address{}
		if tx.Transaction.Transaction.TXType == types.TRANSFER {
			associate_addresses = append(associate_addresses, tx.Transaction.Transaction.To)
			associate_addresses = append(associate_addresses, tx.Transaction.Transaction.From)
		} else if tx.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			for _, rwset := range tx.Transaction.Transaction.RwSet {
				associate_addresses = append(associate_addresses, rwset.Address)
			}
		}
		associate_shards = utils.CalculateShardToSend(associate_addresses)
		decideMajorityTransaction = pb.AddDecidingTransaction(tx.Transaction.Hash, len(associate_shards)-1)
	}

	if tx.Transaction.IsCommit {
		pb.IncreaseVote(decideMajorityTransaction)
	} else {
		pb.SetAbortTransaction(decideMajorityTransaction)
	}
	if !pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		return nil
	}
	if pb.IsDecide(decideMajorityTransaction) {
		if pb.IsAbort(decideMajorityTransaction) {
			log.DecideDebugf("[%v %v] (InitDecideStep) Transaction Abort Process Start for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)
		} else {
			log.DecideDebugf("[%v %v] (InitDecideStep) Decide Commit for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)
		}
		pb.DeleteDecidingTransaction(tx.Transaction.Transaction.Hash)

		pb.decidemu.Lock()
		if pb.decidingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] == nil {
			pb.decidingTransactions[tx.Transaction.Hash] = make(map[int]*message.VotedTransactionWithHeader)
		}
		pb.decidingTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce] = tx
		pb.decidemu.Unlock()

		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.DecideDebugf("[%v %v] (InitDecideStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)

		var decide_vote *quorum.DecideTransactionVote
		if pb.IsAbort(decideMajorityTransaction) {
			decide_vote = quorum.MakeDecideTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, false, tx.Transaction.Nonce, tx.Transaction.OrderCount)
		} else {
			decide_vote = quorum.MakeDecideTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, true, tx.Transaction.Nonce, tx.Transaction.OrderCount)
		}

		// vote is sent to the next leader
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), decide_vote)
		pb.ProcessDecideTransactionVote(decide_vote)

		log.DecideDebugf("[%v %v] (InitDecideStep) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)
	}

	return tx
}

func (pb *PBFT) ProcessDecideStep(leaderVotedTx *message.VotedTransactionWithHeader) {
	log.DecideDebugf("[%v %v] (ProcessDecideStep) Process Decide Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), pb.pm.GetCurView(), pb.pm.GetCurEpoch(), leaderVotedTx.Transaction.Transaction.Hash)
	decideMajorityTransaction := pb.GetDecidingTransaction(leaderVotedTx.Transaction.Hash)
	if decideMajorityTransaction == nil {
		var associate_shards []types.Shard
		associate_addresses := []common.Address{}
		if leaderVotedTx.Transaction.Transaction.TXType == types.TRANSFER {
			associate_addresses = append(associate_addresses, leaderVotedTx.Transaction.Transaction.To)
			associate_addresses = append(associate_addresses, leaderVotedTx.Transaction.Transaction.From)
		} else if leaderVotedTx.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			for _, rwset := range leaderVotedTx.Transaction.Transaction.RwSet {
				associate_addresses = append(associate_addresses, rwset.Address)
			}
		}
		associate_shards = utils.CalculateShardToSend(associate_addresses)
		decideMajorityTransaction = pb.AddDecidingTransaction(leaderVotedTx.Transaction.Hash, len(associate_shards)-1)
	}

	pb.decidemu.Lock()
	if pb.decidingTransactions[leaderVotedTx.Transaction.Hash][leaderVotedTx.Transaction.Nonce] == nil {
		pb.decidingTransactions[leaderVotedTx.Transaction.Hash] = make(map[int]*message.VotedTransactionWithHeader)
	}
	pb.decidingTransactions[leaderVotedTx.Transaction.Transaction.Hash][leaderVotedTx.Transaction.Nonce] = leaderVotedTx

	if qc, ok := pb.decidetransactionbufferedQCs[leaderVotedTx.Transaction.Hash][leaderVotedTx.Transaction.Nonce]; ok {
		pb.processCertificateDecideVote(pb.decidingTransactions[leaderVotedTx.Transaction.Transaction.Hash][leaderVotedTx.Transaction.Nonce], qc)
		pb.decidetransactionbufferedQCs[leaderVotedTx.Transaction.Hash][leaderVotedTx.Transaction.Nonce] = nil
		var decide_vote *quorum.DecideTransactionVote
		if leaderVotedTx.Transaction.IsCommit == !pb.IsAbort(decideMajorityTransaction) {
			if leaderVotedTx.Transaction.IsCommit {
				decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, true, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
			} else {
				decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, false, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
			}
		} else {
			decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, false, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
		}

		// vote is sent to the next leader
		pb.BroadcastToSome(pb.FindCommitteesFor(leaderVotedTx.Header.Epoch), decide_vote)
		return
	} else {
		pb.decidemu.Unlock()
	}

	log.DecideDebugf("[%v %v] Wait Decide!!! Hash %v", pb.ID(), pb.Shard(), leaderVotedTx.Transaction.Hash)
	<-decideMajorityTransaction.IsCollectAll
	pb.DeleteDecidingTransaction(leaderVotedTx.Transaction.Transaction.Hash)
	var decide_vote *quorum.DecideTransactionVote
	if leaderVotedTx.Transaction.IsCommit == !pb.IsAbort(decideMajorityTransaction) {
		if leaderVotedTx.Transaction.IsCommit {
			decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, true, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
		} else {
			decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, false, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
		}
	} else {
		decide_vote = quorum.MakeDecideTransactionVote(leaderVotedTx.Header.Epoch, leaderVotedTx.Header.View, pb.ID(), leaderVotedTx.Transaction.Transaction.Hash, false, leaderVotedTx.Transaction.Nonce, leaderVotedTx.Transaction.OrderCount)
	}

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(leaderVotedTx.Header.Epoch), decide_vote)
	pb.ProcessDecideTransactionVote(decide_vote)

	log.DecideDebugf("[%v %v] (ProcessDecideStep) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), leaderVotedTx.Header.View, leaderVotedTx.Header.Epoch, leaderVotedTx.Transaction.Transaction.Hash)
}

func (pb *PBFT) ProcessDecideTransactionVote(vote *quorum.DecideTransactionVote) {
	log.DecideDebugf("[%v %v] (ProcessDecideTransactionVote) processing vote for view %v epoch %v Hash %v from %v isCommit %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Voter, vote.IsCommit)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessDecideTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessDecideTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.decidetransactionvoteQuorum.Add(vote)
	if !isBuilt {
		log.DecideDebugf("[%v %v] (ProcessDecideTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.DecideDebugf("[%v %v] (ProcessDecideTransactionVote) vote qc is created Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.decidemu.Lock()
	if pb.decidingTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.decidetransactionbufferedQCs[qc.TransactionHash] == nil {
			pb.decidetransactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.DecideTransactionQC)
		}
		pb.decidetransactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.decidemu.Unlock()
		return
	}

	pb.processCertificateDecideVote(pb.decidingTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.DecideDebugf("[%v %v] (ProcessDecideTransactionVote) finished processing vote for view %v epoch %v Hash %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash)
}

func (pb *PBFT) ProcessDecideTransactionCommit(commit *quorum.DecideTransactionCommit) {
	log.DecideDebugf("[%v %v] (ProcessDecideTransactionCommit) processing commit for view %v epoch %v Hash %v from %v isCommit %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash, commit.Voter, commit.IsCommit)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessDecideTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessDecideTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.decidetransactioncommitQuorum.Add(commit)
	if !isBuilt {
		log.DecideDebugf("[%v %v] (ProcessDecideTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.DecideDebugf("[%v %v] (ProcessDecideTransactionCommit) commit cqc is created Hash %v", pb.ID(), pb.Shard(), commit.TransactionHash)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.decidemu.Lock()
	if pb.decidingTransactions[cqc.TransactionHash][cqc.Nonce] == nil || pb.decidingVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.decidetransactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.decidetransactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.DecideTransactionQC)
		}
		pb.decidetransactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.decidemu.Unlock()
		return
	}

	pb.processCertificateDecideCqc(pb.decidingTransactions[cqc.TransactionHash][cqc.Nonce], cqc)

	log.DecideDebugf("[%v %v] (ProcessDecideTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessDecideTransactionAccept(accept *message.DecideTransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	// transactionIsVerified, _ := crypto.PubVerify(accept.Committee_sig[1], crypto.IDToByte(accept.Transaction.Transaction.Transaction.Hash), pb.FindLeaderFor(pb.pm.GetCurView(), pb.pm.GetCurEpoch()))
	// if !transactionIsVerified {
	// 	log.Errorf("[%v %v] (ProcessDecideTransactionAccept) Received a block from leader with an invalid signature", pb.ID())
	// 	return
	// }

	// if accept.Transaction.IsCommit {
	// 	log.DecideDebugf("[%v %v] (ProcessDecideTransactionAccept) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Transaction.Hash)
	// } else {
	// 	// Todo: Process Abort
	// 	log.DecideDebugf("[%v %v] (ProcessDecideTransactionAccept) Not Yet collect all votee transaction for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Transaction.Hash)
	// }
}

func (pb *PBFT) processCertificateDecideVote(decidingTransaction *message.VotedTransactionWithHeader, qc *quorum.DecideTransactionQC) {
	log.DecideDebugf("[%v %v] (processCertificateDecideVote) Start processCertificateDecideVote hash %v", pb.ID(), pb.Shard(), decidingTransaction.Transaction.Transaction.Hash)
	if pb.decidingVoteTransactions[qc.TransactionHash] == nil {
		pb.decidingVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.DecideTransactionQC)
	}
	pb.decidingVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.decidetransactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateDecideCqc(decidingTransaction, cqc)
		pb.decidetransactionbufferedCQCs[qc.TransactionHash][qc.Nonce] = nil
	} else {
		pb.decidemu.Unlock()
	}

	commit := quorum.MakeDecideTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)
	// log.DecideDebugf("[%v %v] (ProcessDecideTransactionVote) Broadcast Commit Message To %v view %v epoch %v Hash %v", pb.ID(), pb.Shard(), pb.FindCommitteesFor(commit.Epoch), vote.View, vote.Epoch, vote.TransactionHash)

	pb.ProcessDecideTransactionCommit(commit)
}

func (pb *PBFT) processCertificateDecideCqc(decidingTransaction *message.VotedTransactionWithHeader, cqc *quorum.DecideTransactionQC) {
	log.DecideDebugf("[%v %v] (processCertificateDecideCqc) Start processCertificateDecideCqc hash %v", pb.ID(), pb.Shard(), decidingTransaction.Transaction.Transaction.Hash)

	// If Leader, Accept Message Broadcast To Validator
	if cqc.Leader == pb.ID() {
		accept := &message.DecideTransactionAccept{
			Header:      pb.decidingTransactions[cqc.TransactionHash][cqc.Nonce].Header,
			Transaction: pb.decidingTransactions[cqc.TransactionHash][cqc.Nonce].Transaction,
		}
		pb.decidemu.Unlock()
		if accept.Transaction.IsCommit {
			validators := pb.FindValidatorsFor(cqc.Epoch)
			pb.BroadcastToSome(validators, accept)
		}
		var vote_complete_transaction message.CommittedTransaction
		vote_complete_transaction.RootShardVotedTransaction.Transaction = accept.Transaction.Transaction
		if accept.Transaction.IsCommit {
			vote_complete_transaction.RootShardVotedTransaction.IsCommit = true
		} else {
			vote_complete_transaction.RootShardVotedTransaction.IsCommit = false
		}
		pb.SendToBlockBuilder(vote_complete_transaction)
		// log.DecideDebugf("[%v %v] (ProcessDecideTransactionCommit) Send Decided Transaction completed consensus To BlockBuilder Tx Hash: %v", pb.ID(), pb.Shard(), cqc.TransactionHash)
	} else {
		pb.decidemu.Unlock()
	}

}
