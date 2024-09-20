package pbft

import (
	"time"
	"unishard/crypto"
	"unishard/evm/core"
	"unishard/log"
	"unishard/message"
	"unishard/quorum"
	"unishard/types"
	"unishard/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/holiman/uint256"
)

func (pb *PBFT) IsRootShard(to common.Address) bool {
	var address []common.Address
	address = append(address, to)
	if utils.CalculateShardToSend(address)[0] == pb.Shard() {
		return true
	} else {
		return false
	}
}

func (pb *PBFT) InitVoteStep(tx *message.SignedTransactionWithHeader) *message.SignedTransactionWithHeader {
	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		if pb.txBlockFlag {
			if tx.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(tx.Transaction.From) {
					go pb.WaitTransactionAddLock(&tx.Transaction.Transaction, true)
					return nil
				} else {
					pb.AddConsensusTx(tx.Transaction.Hash)
				}
			} else if tx.Transaction.TXType == types.SMARTCONTRACT {
				if pb.IsRootShard(tx.Transaction.To) {
					go pb.WaitTransactionAddLock(&tx.Transaction.Transaction, true)
					return nil
				} else {
					pb.AddConsensusTx(tx.Transaction.Hash)
				}
			}
		} else {
			pb.AddConsensusTx(tx.Transaction.Hash)
		}
		if tx.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.From) {
				tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.LatencyDissection.BlockWaitingTime = tx.Transaction.LatencyDissection.BlockWaitingTime + tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli()
			}
		} else if tx.Transaction.TXType == types.SMARTCONTRACT {
			if pb.IsRootShard(tx.Transaction.To) {
				tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.LatencyDissection.BlockWaitingTime = tx.Transaction.LatencyDissection.BlockWaitingTime + tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
				tx.Transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli()
			}
		}
	}
	log.VoteDebugf("[%v %v] (InitVoteStep) Init Vote Step for view %v epoch %v Hash: %v, Nonce: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash, tx.Transaction.Transaction.Nonce)

	// Lock Table Inspect
	if tx.Transaction.Transaction.TXType == types.TRANSFER {
		if pb.IsRootShard(tx.Transaction.From) { // Root Shard
			pb.executemu.Lock()
			if !core.CanTransfer(pb.bc.GetStateDB(), tx.Transaction.Transaction.From, uint256.NewInt(uint64(tx.Transaction.Transaction.Value))) {
				// Abort ignore
				tx.Transaction.Transaction.IsAbort = true
			}
			pb.executemu.Unlock()
			if isCommit := pb.addAddressLockTable(tx.Transaction.Transaction.From, tx.Transaction.Transaction.Hash); !isCommit {
				tx.Transaction.Transaction.IsAbort = true
			} else {
				tx.Transaction.Transaction.IsAbort = false
			}
		} else if pb.IsRootShard(tx.Transaction.To) { // Associate Shard
			if isCommit := pb.addAddressLockTable(tx.Transaction.Transaction.To, tx.Transaction.Transaction.Hash); !isCommit {
				tx.Transaction.Transaction.IsAbort = true
			} else {
				tx.Transaction.Transaction.IsAbort = false
			}
		}
	} else if tx.Transaction.Transaction.TXType == types.SMARTCONTRACT {
		if isCommit := pb.addLockTable(tx.Transaction.Transaction.RwSet, tx.Transaction.Transaction.Hash); !isCommit {
			if pb.IsRootShard(tx.Transaction.Transaction.To) {
				tx.Transaction.Transaction.IsAbort = true
			} else {
				tx.Transaction.Transaction.IsAbort = true
			}
		} else {
			tx.Transaction.Transaction.IsAbort = false
		}
	}
	if !tx.Transaction.Transaction.IsAbort {
		log.VoteDebugf("[%v %v] (InitVoteStep) Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	} else {
		log.VoteDebugf("[%v %v] (InitVoteStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	}

	// only for logging
	// for tx_hash, locks := range pb.lockTable_transaction {
	// 	log.LockDebugf("[%v %v] (InitVoteStep) TransactionHash: %v, Lock Table: %v", pb.Shard(), pb.ID(), tx_hash, &locks)
	// }
	// for addr, table := range pb.lockTable_variable {
	// 	log.LockDebugf("[%v %v] (InitVoteStep) Address: %v, Lock Table: %v", pb.Shard(), pb.ID(), addr, &table)
	// }

	pb.agreemu.Lock()
	if pb.agreeingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] == nil {
		pb.agreeingTransactions[tx.Transaction.Hash] = make(map[int]*message.SignedTransactionWithHeader)
	}
	pb.agreeingTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce] = tx

	if qc, ok := pb.transactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce]; ok {
		pb.processCertificateTransactionVote(pb.agreeingTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce], qc)
		pb.agreemu.Lock()
		pb.transactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce] = nil
		pb.agreemu.Unlock()
		vote := quorum.MakeTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, !tx.Transaction.Transaction.IsAbort, tx.Transaction.Nonce, tx.Transaction.OrderCount)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
		return nil
	} else {
		pb.agreemu.Unlock()
	}

	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.VoteDebugf("[%v %v] (InitVoteStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	}

	vote := quorum.MakeTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, !tx.Transaction.Transaction.IsAbort, tx.Transaction.Nonce, tx.Transaction.OrderCount)

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
	pb.ProcessTransactionVote(vote)

	log.VoteDebugf("[%v %v] (InitVoteStep) Finished preprepared step for view %v epoch %v Hash: %v, vote Iscommit: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash, vote.IsCommit)

	return tx
}

func (pb *PBFT) ProcessTransactionVote(vote *quorum.TransactionVote) {
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) processing vote for view %v epoch %v Hash %v from %v, iscommit: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Voter, vote.IsCommit)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.transactionvoteQuorum.Add(vote)
	if !isBuilt {
		// log.VoteDebugf("[%v %v] (ProcessTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) vote qc is created Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.agreemu.Lock()
	if pb.agreeingTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.transactionbufferedQCs[qc.TransactionHash] == nil {
			pb.transactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.TransactionQC)
		}
		pb.transactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.agreemu.Unlock()
		return
	}

	pb.processCertificateTransactionVote(pb.agreeingTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.VoteDebugf("[%v %v] (ProcessTransactionVote) finished processing vote for view %v epoch %v Hash %v, isCommit: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.IsCommit)
}

func (pb *PBFT) ProcessTransactionCommit(commit *quorum.TransactionCommit) {
	log.VoteDebugf("[%v %v] (ProcessTransactionCommit) processing commit for view %v epoch %v Hash %v from %v iscommit: %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash, commit.Voter, commit.IsCommit)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.transactioncommitQuorum.Add(commit)
	if !isBuilt {
		// log.VoteDebugf("[%v %v] (ProcessTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.VoteDebugf("[%v %v] (ProcessTransactionVote) commit cqc is created Hash %v", pb.ID(), pb.Shard(), commit.TransactionHash)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.agreemu.Lock()
	if pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce] == nil || pb.agreeingVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.transactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.transactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.TransactionQC)
		}
		pb.transactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.agreemu.Unlock()
		return
	}

	pb.processCertificateTransactionCqc(pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce], cqc)

	log.VoteDebugf("[%v %v] (ProcessTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessTransactionAccept(accept *message.TransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	transactionIsVerified, _ := crypto.PubVerify(accept.Transaction.Committee_sig[1], crypto.IDToByte(accept.Transaction.Transaction.Hash))
	if !transactionIsVerified {
		// log.Errorf("[%v %v] (ProcessTransactionAccept) Received a transaction accept from leader with an invalid signature", pb.Shard(), pb.ID())
		return
	}

	if accept.Transaction.Transaction.TXType == types.TRANSFER {
		if pb.IsRootShard(accept.Transaction.From) { // Root Shard
			pb.executemu.Lock()
			if !core.CanTransfer(pb.bc.GetStateDB(), accept.Transaction.Transaction.From, uint256.NewInt(uint64(accept.Transaction.Transaction.Value))) {
				// Abort ignore
				return
			}
			pb.executemu.Unlock()
			if isCommit := pb.addAddressLockTable(accept.Transaction.Transaction.From, accept.Transaction.Transaction.Hash); !isCommit {
				return
			}
		} else { // Associate Shard
			if isCommit := pb.addAddressLockTable(accept.Transaction.Transaction.To, accept.Transaction.Transaction.Hash); !isCommit {
				return
			}
		}
	} else if accept.Transaction.Transaction.TXType == types.SMARTCONTRACT {
		if isCommit := pb.addLockTable(accept.Transaction.Transaction.RwSet, accept.Transaction.Transaction.Hash); !isCommit {
			if pb.IsRootShard(accept.Transaction.Transaction.To) {
				return
			} else {
				return
			}
		}
	}

	var associate_shards []types.Shard
	associate_addresses := []common.Address{}
	for _, rwset := range accept.Transaction.Transaction.RwSet {
		associate_addresses = append(associate_addresses, rwset.Address)
	}
	associate_shards = utils.CalculateShardToSend(associate_addresses)
	pb.AddDecidingTransaction(accept.Transaction.Transaction.Hash, len(associate_shards)-1)
}

func (pb *PBFT) processCertificateTransactionVote(votingTransaction *message.SignedTransactionWithHeader, qc *quorum.TransactionQC) {
	log.VoteDebugf("[%v %v] (processCertificateTransactionVote) Start processCertificateTransactionVote hash %v", pb.ID(), pb.Shard(), votingTransaction.Transaction.Transaction.Hash)
	if pb.agreeingVoteTransactions[qc.TransactionHash] == nil {
		pb.agreeingVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.TransactionQC)
	}
	pb.agreeingVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.transactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateTransactionCqc(votingTransaction, cqc)
		cqc = nil
	} else {
		pb.agreemu.Unlock()
	}

	commit := quorum.MakeTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessTransactionCommit(commit)
}

func (pb *PBFT) processCertificateTransactionCqc(votingTransaction *message.SignedTransactionWithHeader, cqc *quorum.TransactionQC) {
	log.VoteDebugf("[%v %v] (processCertificateTransactionCqc) Start processCertificateTransactionCqc hash %v", pb.ID(), pb.Shard(), votingTransaction.Transaction.Transaction.Hash)
	var associate_shards []types.Shard
	associate_addresses := []common.Address{}
	if votingTransaction.Transaction.Transaction.TXType == types.TRANSFER {
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.Transaction.To)
		associate_addresses = append(associate_addresses, votingTransaction.Transaction.Transaction.From)
	} else if votingTransaction.Transaction.Transaction.TXType == types.SMARTCONTRACT {
		for _, rwset := range votingTransaction.Transaction.Transaction.RwSet {
			associate_addresses = append(associate_addresses, rwset.Address)
		}
	}
	associate_shards = utils.CalculateShardToSend(associate_addresses)
	pb.AddDecidingTransaction(pb.agreeingTransactions[cqc.TransactionHash][cqc.Nonce].Transaction.Transaction.Hash, len(associate_shards)-1)

	if !cqc.IsCommit {
		if votingTransaction.Transaction.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(votingTransaction.Transaction.Transaction.From) { // Root Shard
				pb.deleteAddressLockTable(votingTransaction.Transaction.Transaction.From, votingTransaction.Transaction.Transaction.Hash)
			} else if pb.IsRootShard(votingTransaction.Transaction.Transaction.To) { // Associate Shard
				pb.deleteAddressLockTable(votingTransaction.Transaction.Transaction.To, votingTransaction.Transaction.Transaction.Hash)
			}
		} else if votingTransaction.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			pb.deleteLockTable(votingTransaction.Transaction.Transaction.RwSet, votingTransaction.Transaction.Transaction.Hash)
		}
	}

	// If Leader, Accept Message Broadcast To Validator
	if cqc.Leader == pb.ID() {
		accept := &message.TransactionAccept{
			SignedTransactionWithHeader: message.SignedTransactionWithHeader{
				Header:      votingTransaction.Header,
				Transaction: votingTransaction.Transaction,
			},
		}
		pb.agreemu.Unlock()
		if cqc.IsCommit {
			accept.IsCommit = true
		} else {
			accept.IsCommit = false
		}
		validators := pb.FindValidatorsFor(cqc.Epoch)
		pb.BroadcastToSome(validators, accept)
		var vote_complete_transaction message.RootShardVotedTransaction
		vote_complete_transaction.Transaction = accept.Transaction.Transaction
		if cqc.IsCommit {
			vote_complete_transaction.IsCommit = true
		} else {
			vote_complete_transaction.IsCommit = false
		}

		pb.SendToBlockBuilder(vote_complete_transaction)
		if !vote_complete_transaction.IsCommit {
			vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime = time.Now().UnixMilli() - vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime
			vote_complete_transaction.LatencyDissection.RootVoteConsensusTime = vote_complete_transaction.LatencyDissection.RootVoteConsensusTime + vote_complete_transaction.AbortAndRetryLatencyDissection.RootVoteConsensusTime
			vote_complete_transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
			if vote_complete_transaction.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(vote_complete_transaction.Transaction.From) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
					pb.SubConsensusTx(vote_complete_transaction.Transaction.Hash)
				}
			} else if vote_complete_transaction.Transaction.TXType == types.SMARTCONTRACT {
				if pb.IsRootShard(vote_complete_transaction.Transaction.To) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
					pb.SubConsensusTx(vote_complete_transaction.Transaction.Hash)
				}
			}
		}
		log.VoteDebugf("[%v %v] (processCertificateTransactionCqc) Send Transaction completed consensus To BlockBuilder Tx Hash: %v, IsCommit: %v", pb.ID(), pb.Shard(), cqc.TransactionHash, cqc.IsCommit)
	} else {
		pb.agreemu.Unlock()
	}
}
