package pbft

import (
	"time"
	"unishard/crypto"
	"unishard/evm"
	"unishard/evm/vm/runtime"
	"unishard/log"
	"unishard/message"
	"unishard/quorum"
	"unishard/types"

	"github.com/holiman/uint256"
)

func (pb *PBFT) InitCommitStep(tx *message.CommittedTransactionWithHeader) *message.CommittedTransactionWithHeader {
	if !tx.Transaction.IsCommit {
		log.CommitDebugf("[%v %v] (InitCommitStep) Init Abort Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)
		if tx.Transaction.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.From) { // Root Shard
				pb.deleteAddressLockTable(tx.Transaction.Transaction.From, tx.Transaction.Transaction.Hash)
				log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
			} else if pb.IsRootShard(tx.Transaction.To) { // Associate Shard
				pb.deleteAddressLockTable(tx.Transaction.Transaction.To, tx.Transaction.Transaction.Hash)
				log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
			}
		} else if tx.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			pb.deleteLockTable(tx.Transaction.Transaction.RwSet, tx.Transaction.Transaction.Hash)
			log.CommitDebugf("[%v %v] (InitCommitStep) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
		}
	} else {
		log.CommitDebugf("[%v %v] (InitCommitStep) Init Commit Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)
		// Execute
		cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
		// tdb := triedb.NewDatabase(pb.leveldb, nil)
		// statedb := state.NewDatabaseWithNodeDB(pb.leveldb, tdb)
		// initState, _ := state.New(common.HexToHash(utils.GetRootHash(pb.Shard())), statedb, nil)
		initState := pb.bc.GetStateDB()
		evm_machine := runtime.NewEnv(cfg, initState)
		if tx.Transaction.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(tx.Transaction.Transaction.From) {
				pb.executemu.Lock()
				initState.SubBalance(tx.Transaction.Transaction.From, uint256.NewInt(uint64(tx.Transaction.Transaction.Value)))
				pb.executemu.Unlock()
				// pb.deleteAddressLockTable(tx.Transaction.Transaction.From, tx.Transaction.Transaction.Hash)
				// log.StateInfof("[%v %v] (InitCommitStep) SubBalance From address %v, %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash, tx.Transaction.Transaction.From)
			} else if pb.IsRootShard(tx.Transaction.Transaction.To) {
				pb.executemu.Lock()
				initState.AddBalance(tx.Transaction.Transaction.To, uint256.NewInt(uint64(tx.Transaction.Transaction.Value)))
				pb.executemu.Unlock()
				// pb.deleteAddressLockTable(tx.Transaction.Transaction.To, tx.Transaction.Transaction.Hash)
				// log.StateInfof("[%v %v] (InitCommitStep) AddBalance To address %v, %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash, tx.Transaction.Transaction.To)
			}
		} else if tx.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			if !pb.IsRootShard(tx.Transaction.From) {
				pb.executemu.Lock()
				initState.CreateAccount(tx.Transaction.From)
				initState.SetBalance(tx.Transaction.From, uint256.NewInt(1e18))
				pb.executemu.Unlock()
			}
			for _, rwset := range tx.Transaction.Transaction.RwSet {
				if pb.IsRootShard(rwset.Address) {
					pb.executemu.Lock()
					_, _, err := evm.Execute(evm_machine, initState, rwset.Data, tx.Transaction.From, rwset.Address)
					pb.executemu.Unlock()
					if err != nil {
						log.Errorf("[%v %v] (InitCommitStep) Error Execute Cross Smart Contract: %v Hash: %v Address: %v", pb.ID(), pb.Shard(), err, tx.Transaction.Transaction.Hash, rwset.Address)
						continue
					}
					// log.Errorf("[%v %v] (InitCommitStep) Success Execute Cross Smart Contract: %v Hash: %v Address: %v", pb.ID(), pb.Shard(), err, tx.Transaction.Transaction.Hash, rwset.Address)
				}
			}
			// pb.deleteLockTable(tx.Transaction.Transaction.RwSet, tx.Transaction.Transaction.Hash)
			// log.StateInfof("[%v %v] (InitCommitStep) Execute Smart Contract address %v, %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash, tx.Transaction.Transaction.To)
		}
		// if tx.Transaction.TXType == types.TRANSFER {
		// 	if pb.IsRootShard(tx.Transaction.From) {
		// 		pb.deleteAddressLockTable(tx.Transaction.From, tx.Transaction.Hash)
		// 	} else if pb.IsRootShard(tx.Transaction.To) {
		// 		pb.deleteAddressLockTable(tx.Transaction.To, tx.Transaction.Hash)
		// 	}
		// } else if tx.Transaction.TXType == types.SMARTCONTRACT {
		// 	pb.deleteLockTable(tx.Transaction.RwSet, tx.Transaction.Hash)
		// }
	}

	// only for logging
	// for tx_hash, locks := range pb.lockTable_transaction {
	// 	log.LockDebugf("[%v %v] (InitCommitStep) TransactionHash: %v, Lock Table: %v", pb.ID(), pb.Shard(), tx_hash, &locks)
	// }
	// for addr, table := range pb.lockTable_variable {
	// 	log.LockDebugf("[%v %v] (InitCommitStep) Address: %v, Lock Table: %v", pb.ID(), pb.Shard(), addr, &table)
	// }

	pb.commitmu.Lock()
	if pb.committingTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] == nil {
		pb.committingTransactions[tx.Transaction.Hash] = make(map[int]*message.CommittedTransactionWithHeader)
	}
	pb.committingTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce] = tx

	if qc, ok := pb.committransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce]; ok {
		pb.processCertificateCommitVote(pb.committingTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce], qc)
		pb.committransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce] = nil
		vote := quorum.MakeCommitTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, tx.Transaction.IsCommit, tx.Transaction.Nonce, tx.Transaction.OrderCount)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
		return nil
	} else {
		pb.commitmu.Unlock()
	}

	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.CommitDebugf("[%v %v] (InitCommitStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	}

	vote := quorum.MakeCommitTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, tx.Transaction.IsCommit, tx.Transaction.Nonce, tx.Transaction.OrderCount)

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
	pb.ProcessCommitTransactionVote(vote)

	log.CommitDebugf("[%v %v] (InitCommitStep) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)

	return tx
}

func (pb *PBFT) ProcessCommitTransactionVote(vote *quorum.CommitTransactionVote) {
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) processing vote for view %v epoch %v Hash %v from %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Voter)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessCommitTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessCommitTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.committransactionvoteQuorum.Add(vote)
	if !isBuilt {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) vote qc is created Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.commitmu.Lock()
	if pb.committingTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.committransactionbufferedQCs[qc.TransactionHash] == nil {
			pb.committransactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
		}
		pb.committransactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.commitmu.Unlock()
		return
	}

	pb.processCertificateCommitVote(pb.committingTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.CommitDebugf("[%v %v] (ProcessCommitTransactionVote) finished processing vote for view %v epoch %v Hash %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash)
}

func (pb *PBFT) ProcessCommitTransactionCommit(commit *quorum.CommitTransactionCommit) {
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) processing commit for view %v epoch %v Hash %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessCommitTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessCommitTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.committransactioncommitQuorum.Add(commit)
	if !isBuilt {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) commit cqc is created Hash %v", pb.ID(), pb.Shard(), commit.TransactionHash)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.commitmu.Lock()
	if pb.committingTransactions[cqc.TransactionHash][cqc.Nonce] == nil || pb.committingVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.committransactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.committransactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
		}
		pb.committransactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.commitmu.Unlock()
		return
	}

	pb.processCertificateCommitCqc(pb.committingTransactions[cqc.TransactionHash][cqc.Nonce], cqc)
	log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessCommitTransactionAccept(accept *message.CommitTransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	// transactionIsVerified, _ := crypto.PubVerify(accept.Transaction.Committee_sig[1], crypto.IDToByte(accept.Transaction.Transaction.Hash), pb.FindLeaderFor(pb.pm.GetCurView(), pb.pm.GetCurEpoch()))
	// if !transactionIsVerified {
	// 	log.Errorf("[%v %v] (ProcessTransactionAccept) Received a block from leader with an invalid signature", pb.ID())
	// 	return
	// }

	if !accept.Transaction.IsCommit {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Init Abort Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Transaction.Hash)
		if len(pb.lockTable_transaction[accept.Transaction.Transaction.Hash]) > 0 {
			if accept.Transaction.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(accept.Transaction.Transaction.From) { // Root Shard
					pb.deleteAddressLockTable(accept.Transaction.Transaction.From, accept.Transaction.Transaction.Hash)
					log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash)
				} else if pb.IsRootShard(accept.Transaction.Transaction.To) { // Associate Shard
					pb.deleteAddressLockTable(accept.Transaction.Transaction.To, accept.Transaction.Transaction.Hash)
					log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash)
				}
			} else if accept.Transaction.Transaction.TXType == types.SMARTCONTRACT {
				pb.deleteLockTable(accept.Transaction.Transaction.RwSet, accept.Transaction.Transaction.Hash)
				log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash)
				// if pb.IsRootShard(accept.Transaction.Transaction.To) {
				// 	log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Release Lock and Go to wait Transaction Hash: %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash)
				// }
			}
		}
	} else {
		log.CommitDebugf("[%v %v] (ProcessCommitTransactionAccept) Init Commit Step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), accept.Header.View, accept.Header.Epoch, accept.Transaction.Transaction.Hash)
		// Execute
		cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
		evm_machine := runtime.NewEnv(cfg, pb.bc.GetStateDB())
		if accept.Transaction.Transaction.TXType == types.TRANSFER {
			if pb.IsRootShard(accept.Transaction.Transaction.From) {
				pb.executemu.Lock()
				pb.bc.GetStateDB().SubBalance(accept.Transaction.Transaction.From, uint256.NewInt(uint64(accept.Transaction.Transaction.Value)))
				pb.executemu.Unlock()
				// pb.deleteAddressLockTable(accept.Transaction.Transaction.From, accept.Transaction.Transaction.Hash)
				// log.StateInfof("[%v %v] (ProcessCommitTransactionAccept) SubBalance From address %v, %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash, accept.Transaction.Transaction.From)
			} else if pb.IsRootShard(accept.Transaction.Transaction.To) {
				pb.executemu.Lock()
				pb.bc.GetStateDB().AddBalance(accept.Transaction.Transaction.To, uint256.NewInt(uint64(accept.Transaction.Transaction.Value)))
				pb.executemu.Unlock()
				// pb.deleteAddressLockTable(accept.Transaction.Transaction.To, accept.Transaction.Transaction.Hash)
				// log.StateInfof("[%v %v] (ProcessCommitTransactionAccept) AddBalance To address %v, %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash, accept.Transaction.Transaction.To)
			}
		} else if accept.Transaction.Transaction.TXType == types.SMARTCONTRACT {
			for _, rwset := range accept.Transaction.Transaction.RwSet {
				if pb.IsRootShard(rwset.Address) {
					// 락 해제하기 Write를 가장 나중에 해제해야함
					fdcm := 0
					if !pb.IsRootShard(accept.Transaction.Transaction.From) {
						pb.executemu.Lock()
						pb.bc.GetStateDB().CreateAccount(accept.Transaction.Transaction.From)
						pb.bc.GetStateDB().SetBalance(accept.Transaction.Transaction.From, uint256.NewInt(1e18))
						pb.executemu.Unlock()
						fdcm++
					}
					pb.executemu.Lock()
					_, _, err := evm.Execute(evm_machine, pb.bc.GetStateDB(), rwset.Data, accept.Transaction.Transaction.From, rwset.Address)
					pb.executemu.Unlock()
					if err != nil {
						log.Errorf("[%v %v] (ProcessCommitTransactionAccept) Error Execute Cross Smart Contract: %v", pb.ID(), pb.Shard(), err)
						continue
					}
					if fdcm != 0 {
						pb.executemu.Lock()
						pb.bc.GetStateDB().SelfDestruct(accept.Transaction.Transaction.From)
						pb.executemu.Unlock()
					}
				}
			}
			pb.deleteLockTable(accept.Transaction.Transaction.RwSet, accept.Transaction.Transaction.Hash)
			// log.StateInfof("[%v %v] (ProcessCommitTransactionAccept) Execute Smart Contract address %v, %v", pb.ID(), pb.Shard(), accept.Transaction.Transaction.Hash, accept.Transaction.Transaction.To)
		}
	}
}

func (pb *PBFT) processCertificateCommitVote(committingTransaction *message.CommittedTransactionWithHeader, qc *quorum.CommitTransactionQC) {
	log.CommitDebugf("[%v %v] (processCertificateCommitVote) Start processCertificateCommitVote hash %v", pb.ID(), pb.Shard(), committingTransaction.Transaction.Transaction.Hash)
	if pb.committingVoteTransactions[qc.TransactionHash] == nil {
		pb.committingVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.CommitTransactionQC)
	}
	pb.committingVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.committransactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateCommitCqc(committingTransaction, cqc)
		pb.committransactionbufferedCQCs[qc.TransactionHash][qc.Nonce] = nil
	} else {
		pb.commitmu.Unlock()
	}

	commit := quorum.MakeCommitTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessCommitTransactionCommit(commit)
}

func (pb *PBFT) processCertificateCommitCqc(committingTransaction *message.CommittedTransactionWithHeader, cqc *quorum.CommitTransactionQC) {
	log.CommitDebugf("[%v %v] (processCertificateCommitCqc) Start processCertificateCommitCqc hash %v", pb.ID(), pb.Shard(), committingTransaction.Transaction.Transaction.Hash)
	// If Leader, Accept Message Broadcast To Validator
	if cqc.Leader == pb.ID() {
		accept := &message.CommitTransactionAccept{
			Header:      pb.committingTransactions[cqc.TransactionHash][cqc.Nonce].Header,
			Transaction: pb.committingTransactions[cqc.TransactionHash][cqc.Nonce].Transaction,
		}
		pb.commitmu.Unlock()
		validators := pb.FindValidatorsFor(cqc.Epoch)
		pb.BroadcastToSome(validators, accept)
		var vote_complete_transaction message.CompleteTransaction
		vote_complete_transaction.Transaction = accept.Transaction.Transaction
		vote_complete_transaction.IsCommit = accept.Transaction.IsCommit
		vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime = time.Now().UnixMilli() - vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime
		vote_complete_transaction.LatencyDissection.CommitConsensusTime = vote_complete_transaction.LatencyDissection.CommitConsensusTime + vote_complete_transaction.AbortAndRetryLatencyDissection.CommitConsensusTime
		vote_complete_transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
		if vote_complete_transaction.IsCommit {
			vote_complete_transaction.LatencyDissection.ProcessTime = time.Now().UnixMilli() - vote_complete_transaction.StartTime
			pb.committed_transactions.AddTxn(&vote_complete_transaction.Transaction)
			log.CommitDebugf("[%v %v] (ProcessCommitTransactionCommit) Add completed consensus Transaction Tx Hash: %v, IsCommit: %v", pb.ID(), pb.Shard(), cqc.TransactionHash, cqc.IsCommit)
		} else {
			if vote_complete_transaction.Transaction.TXType == types.TRANSFER {
				if pb.IsRootShard(vote_complete_transaction.Transaction.From) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
				}
			} else if vote_complete_transaction.Transaction.TXType == types.SMARTCONTRACT {
				if pb.IsRootShard(vote_complete_transaction.Transaction.To) {
					go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
				}
			}
		}
		pb.SubConsensusTx(cqc.TransactionHash)
	} else {
		pb.commitmu.Unlock()
	}
}
