package pbft

import (
	"time"
	"unishard/crypto"
	"unishard/evm"
	"unishard/evm/core"
	"unishard/evm/state"
	"unishard/evm/vm"
	"unishard/evm/vm/runtime"
	"unishard/log"
	"unishard/message"
	"unishard/quorum"
	"unishard/types"

	"github.com/holiman/uint256"
)

type LocalPBFT struct {
	Transaction *message.SignedTransactionWithHeader
	IsVoted     bool
	BufferedQC  *quorum.LocalTransactionQC
	BufferedCQC *quorum.LocalTransactionQC
}

func executeTransaction(state *state.StateDB, evm_machine *vm.EVM, lt *message.Transaction) (bool, error) {
	if lt.TXType == types.TRANSFER {
		err := evm.Transfer(state, lt.From, lt.To, uint256.NewInt(uint64(lt.Value)))
		if err != nil {
			return false, err
		}
		// log.StateInfof("[%v] (ProcessLocalTransactionCommit) Local Transfer Transaction Sender: %v %v, Recipient: %v %v", pb.ID(), lt.From, initState.GetBalance(lt.From), lt.To, initState.GetBalance(lt.To))
	} else if lt.TXType == types.SMARTCONTRACT {
		// fmt.Printf("[%v] (ProcessLocalTransactionCommit) Local Travel SmartContract: %v, Data: %x\n", pb.ID(), lt, lt.Data)
		_, _, err := evm.Execute(evm_machine, state, lt.Data, lt.From, lt.To)
		if err != nil {
			return false, err
		}
		// log.StateInfof("[%v] (ProcessLocalTransactionCommit) Local Smart Contract Transaction Sender: %v", pb.ID(), lt.From)
	}
	return true, nil
}

func (pb *PBFT) InitLocalLockStep(tx *message.SignedTransactionWithHeader) *message.SignedTransactionWithHeader {
	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		if pb.txBlockFlag {
			go pb.WaitTransactionAddLock(&tx.Transaction.Transaction, true)
			return nil
		} else {
			pb.AddConsensusTx(tx.Transaction.Hash)
		}
		tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli() - tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
		tx.Transaction.LocalLatencyDissection.BlockWaitingTime = tx.Transaction.LocalLatencyDissection.BlockWaitingTime + tx.Transaction.AbortAndRetryLatencyDissection.BlockWaitingTime
		tx.Transaction.AbortAndRetryLatencyDissection.ConsensusTime = time.Now().UnixMilli()
		tx.Transaction.StartTime = time.Now().UnixMilli()
	}
	log.LocalDebugf("[%v %v] (InitLocalLockStep) Init Vote Step for view %v epoch %v Hash: %v Nonce: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash, tx.Transaction.Transaction.Nonce)

	cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
	// tdb := triedb.NewDatabase(pb.leveldb, nil)
	// statedb := state.NewDatabaseWithNodeDB(pb.leveldb, tdb)
	// initState, _ := state.New(common.HexToHash(utils.GetRootHash(pb.Shard())), statedb, nil)
	initState := pb.bc.GetStateDB()
	evm_machine := runtime.NewEnv(cfg, initState)

	// Lock Table Inspect
	if tx.Transaction.TXType == types.TRANSFER {
		pb.executemu.Lock()
		if !core.CanTransfer(initState, tx.Transaction.From, uint256.NewInt(uint64(tx.Transaction.Value))) {
			// Abort
			tx.Transaction.Transaction.IsAbort = true
			log.LocalDebugf("[%v %v] (InitLocalLockStep) Cannot Transfer Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
		}
		pb.executemu.Unlock()
		if isCommit := pb.addTransferLockTable(tx.Transaction.Transaction, tx.Transaction.Hash); !isCommit {
			tx.Transaction.Transaction.IsAbort = true
			// log.LocalDebugf("[%v %v] (InitLocalLockStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
		} else {
			tx.Transaction.Transaction.IsAbort = false
			pb.executemu.Lock()
			success, err := executeTransaction(initState, evm_machine, &tx.Transaction.Transaction)
			pb.executemu.Unlock()
			if !success {
				log.Errorf("[%v %v] (ProcessLocalTransactionCommit) Error Local Transfer Transaction %v", pb.ID(), pb.Shard(), err)
				return nil
			}
		}
	} else if tx.Transaction.TXType == types.SMARTCONTRACT {
		if isCommit := pb.addLockTable(tx.Transaction.RwSet, tx.Transaction.Hash); !isCommit {
			tx.Transaction.Transaction.IsAbort = true
			// log.LocalDebugf("[%v %v] (InitLocalLockStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
		} else {
			tx.Transaction.Transaction.IsAbort = false
			pb.executemu.Lock()
			success, err := executeTransaction(initState, evm_machine, &tx.Transaction.Transaction)
			pb.executemu.Unlock()
			if !success {
				log.Errorf("[%v] (ProcessLocalTransactionCommit) Error Execute Smart Contract: %v %v", pb.ID(), tx.Transaction.Hash, err)
				return nil
			}
		}
	}
	if !tx.Transaction.Transaction.IsAbort {
		log.LockDebugf("[%v %v] (InitLocalLockStep) Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	} else {
		log.LockDebugf("[%v %v] (InitLocalLockStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	}

	// only for logging
	// for tx_hash, locks := range pb.lockTable_transaction {
	// 	log.LockDebugf("[%v %v] (InitLocalLockStep) TransactionHash: %v, Lock Table: %v", pb.Shard(), pb.ID(), tx_hash, &locks)
	// }
	// for addr, table := range pb.lockTable_variable {
	// 	log.LockDebugf("[%v %v] (InitLocalLockStep) Address: %v, Lock Table: %v", pb.Shard(), pb.ID(), addr, &table)
	// }

	pb.localmu.Lock()
	if pb.agreeingLocalTransactions[tx.Transaction.Hash][tx.Transaction.Nonce] == nil {
		pb.agreeingLocalTransactions[tx.Transaction.Hash] = make(map[int]*message.SignedTransactionWithHeader)
	}
	pb.agreeingLocalTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce] = tx

	if qc, ok := pb.localtransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce]; ok {
		pb.processCertificateLocalVote(pb.agreeingLocalTransactions[tx.Transaction.Transaction.Hash][tx.Transaction.Nonce], qc)
		pb.localtransactionbufferedQCs[tx.Transaction.Hash][tx.Transaction.Nonce] = nil
		vote := quorum.MakeLocalTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, !tx.Transaction.Transaction.IsAbort, tx.Transaction.Nonce, tx.Transaction.OrderCount)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
		return nil
	} else {
		pb.localmu.Unlock()
	}

	if pb.IsLeader(pb.ID(), tx.Header.View, tx.Header.Epoch) {
		// Leader Signing
		leader_signature, _ := crypto.PrivSign(crypto.IDToByte(tx.Transaction.Transaction.Hash), nil)
		tx.Transaction.Committee_sig = append(tx.Transaction.Committee_sig, leader_signature)
		pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), tx)
		log.LocalDebugf("[%v %v] (InitLocalLockStep) Broadcast to committee tx hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Transaction.Hash)
	}

	vote := quorum.MakeLocalTransactionVote(tx.Header.Epoch, tx.Header.View, pb.ID(), tx.Transaction.Transaction.Hash, !tx.Transaction.Transaction.IsAbort, tx.Transaction.Nonce, tx.Transaction.OrderCount)

	// vote is sent to the next leader
	pb.BroadcastToSome(pb.FindCommitteesFor(tx.Header.Epoch), vote)
	pb.ProcessLocalTransactionVote(vote)

	log.LocalDebugf("[%v %v] (InitLocalLockStep) Finished preprepared step for view %v epoch %v Hash: %v", pb.ID(), pb.Shard(), tx.Header.View, tx.Header.Epoch, tx.Transaction.Transaction.Hash)

	return tx
}

func (pb *PBFT) ProcessLocalTransactionVote(vote *quorum.LocalTransactionVote) {
	log.LocalDebugf("[%v %v] (ProcessLocalTransactionVote) processing vote for view %v epoch %v Hash %v from %v iscommit: %v nonce:: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.Voter, vote.IsCommit, vote.Nonce)

	voteIsVerified, err := crypto.PubVerify(vote.Signature, crypto.IDToByte(vote.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessLocalTransactionVote) error in verifying the signature in vote Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	if !voteIsVerified {
		log.Warningf("[%v %v] (ProcessLocalTransactionVote) received a vote with invalid signature Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}

	isBuilt, qc := pb.localtransactionvoteQuorum.Add(vote)
	if !isBuilt {
		// log.LocalDebugf("[%v %v] (ProcessLocalTransactionVote) votes are not sufficient to build a qc Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)
		return
	}
	log.LocalDebugf("[%v %v] (ProcessLocalTransactionVote) vote qc is created Hash %v", pb.ID(), pb.Shard(), vote.TransactionHash)

	qc.Leader = pb.FindLeaderFor(qc.View, qc.Epoch)

	pb.localmu.Lock()
	if pb.agreeingLocalTransactions[qc.TransactionHash][qc.Nonce] == nil {
		if pb.localtransactionbufferedQCs[qc.TransactionHash] == nil {
			pb.localtransactionbufferedQCs[qc.TransactionHash] = make(map[int]*quorum.LocalTransactionQC)
		}
		pb.localtransactionbufferedQCs[qc.TransactionHash][qc.Nonce] = qc
		pb.localmu.Unlock()
		return
	}

	pb.processCertificateLocalVote(pb.agreeingLocalTransactions[qc.TransactionHash][qc.Nonce], qc)

	log.LocalDebugf("[%v %v] (ProcessLocalTransactionVote) finished processing vote for view %v epoch %v Hash %v commit iscommit: %v", pb.ID(), pb.Shard(), vote.View, vote.Epoch, vote.TransactionHash, vote.IsCommit)
}

func (pb *PBFT) ProcessLocalTransactionCommit(commit *quorum.LocalTransactionCommit) {
	log.LocalDebugf("[%v %v] (ProcessLocalTransactionCommit) processing commit for view %v epoch %v Hash %v from %v iscommit: %v nonce: %v", pb.ID(), pb.Shard(), commit.View, commit.Epoch, commit.TransactionHash, commit.Voter, commit.IsCommit, commit.Nonce)
	commitIsVerified, err := crypto.PubVerify(commit.Signature, crypto.IDToByte(commit.TransactionHash))
	if err != nil {
		log.Errorf("[%v %v] (ProcessLocalTransactionCommit) error in verifying the signature in commit ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	if !commitIsVerified {
		log.Warningf("[%v %v] (ProcessLocalTransactionCommit) received a commit with invalid signature ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}

	isBuilt, cqc := pb.localtransactioncommitQuorum.Add(commit)
	if !isBuilt {
		// log.LocalDebugf("[%v %v] (ProcessLocalTransactionCommit) commits are not sufficient to build a cqc ID %v", pb.ID(), pb.Shard(), commit.TransactionHash)
		return
	}
	log.LocalDebugf("[%v %v] (ProcessLocalTransactionCommit) commit cqc is created Hash %v", pb.ID(), pb.Shard(), commit.TransactionHash)

	cqc.Leader = pb.FindLeaderFor(cqc.View, cqc.Epoch)

	pb.localmu.Lock()
	if pb.agreeingLocalVoteTransactions[cqc.TransactionHash][cqc.Nonce] == nil {
		if pb.localtransactionbufferedCQCs[cqc.TransactionHash] == nil {
			pb.localtransactionbufferedCQCs[cqc.TransactionHash] = make(map[int]*quorum.LocalTransactionQC)
		}
		pb.localtransactionbufferedCQCs[cqc.TransactionHash][cqc.Nonce] = cqc
		pb.localmu.Unlock()
		return
	}

	pb.processCertificateLocalCqc(pb.agreeingLocalTransactions[cqc.TransactionHash][cqc.Nonce], cqc)
	log.LocalDebugf("[%v %v] (ProcessLocalTransactionCommit) finished processing commit for view %v epoch %v ID %v", pb.ID(), pb.Shard(), cqc.View, cqc.Epoch, cqc.TransactionHash)
}

func (pb *PBFT) ProcessLocalTransactionAccept(accept *message.TransactionAccept) {
	if pb.IsCommittee(pb.ID(), accept.Header.Epoch) {
		return
	}

	// 실행
	lt := accept.Transaction
	if !accept.IsCommit {
		// log.LocalDebugf("[%v %v] (ProcessLocalTransactionAccept) Go to Wait Transaction Hash: %v", pb.ID(), pb.Shard(), lt.Hash)
	} else {
		cfg := evm.SetConfig(100, string(rune(time.Now().Unix())))
		// tdb := triedb.NewDatabase(pb.leveldb, nil)
		// statedb := state.NewDatabaseWithNodeDB(pb.leveldb, tdb)
		// initState, _ := state.New(common.HexToHash(utils.GetRootHash(pb.Shard())), statedb, nil)
		initState := pb.bc.GetStateDB()
		evm_machine := runtime.NewEnv(cfg, initState)

		// Lock Table Inspect
		if lt.Transaction.TXType == types.TRANSFER {
			pb.executemu.Lock()
			if !core.CanTransfer(initState, lt.Transaction.From, uint256.NewInt(uint64(lt.Transaction.Value))) {
				// Abort
				lt.Transaction.IsAbort = true
				// log.LocalDebugf("[%v %v] (InitLocalLockStep) Cannot Transfer Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
			}
			pb.executemu.Unlock()
			if isCommit := pb.addTransferLockTable(lt.Transaction, lt.Transaction.Hash); !isCommit {
				lt.Transaction.IsAbort = true
				// log.LocalDebugf("[%v %v] (InitLocalLockStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
			} else {
				lt.Transaction.IsAbort = false
				pb.executemu.Lock()
				success, err := executeTransaction(initState, evm_machine, &lt.Transaction)
				pb.executemu.Unlock()
				if !success {
					log.Errorf("[%v] (ProcessLocalTransactionCommit) Error Local Transfer Transaction %v", pb.ID(), err)
					return
				}
			}
		} else if lt.Transaction.TXType == types.SMARTCONTRACT {
			if isCommit := pb.addLockTable(lt.Transaction.RwSet, lt.Transaction.Hash); !isCommit {
				lt.Transaction.IsAbort = true
				// log.LocalDebugf("[%v %v] (InitLocalLockStep) Not Acquire Lock Transaction Hash: %v", pb.ID(), pb.Shard(), tx.Transaction.Hash)
			} else {
				lt.Transaction.IsAbort = false
				pb.executemu.Lock()
				success, err := executeTransaction(initState, evm_machine, &lt.Transaction)
				pb.executemu.Unlock()
				if !success {
					log.Errorf("[%v] (ProcessLocalTransactionCommit) Error Execute Smart Contract: %v %v", pb.ID(), lt.Transaction.Hash, err)
					return
				}
			}
		}
	}
}

func (pb *PBFT) processCertificateLocalVote(processingLocalTransaction *message.SignedTransactionWithHeader, qc *quorum.LocalTransactionQC) {
	log.LocalDebugf("[%v %v] (processCertificateLocalVote) Start processCertificateLocalVote hash %v", pb.ID(), pb.Shard(), processingLocalTransaction.Transaction.Transaction.Hash)

	if pb.agreeingLocalVoteTransactions[qc.TransactionHash] == nil {
		pb.agreeingLocalVoteTransactions[qc.TransactionHash] = make(map[int]*quorum.LocalTransactionQC)
	}
	pb.agreeingLocalVoteTransactions[qc.TransactionHash][qc.Nonce] = qc

	if cqc, ok := pb.localtransactionbufferedCQCs[qc.TransactionHash][qc.Nonce]; ok {
		pb.processCertificateLocalCqc(processingLocalTransaction, cqc)
		pb.localtransactionbufferedCQCs[qc.TransactionHash][qc.Nonce] = nil
	} else {
		pb.localmu.Unlock()
	}

	commit := quorum.MakeLocalTransactionCommit(qc.Epoch, qc.View, pb.ID(), qc.TransactionHash, qc.IsCommit, qc.Nonce, qc.OrderCount)
	pb.BroadcastToSome(pb.FindCommitteesFor(commit.Epoch), commit)

	pb.ProcessLocalTransactionCommit(commit)
}

func (pb *PBFT) processCertificateLocalCqc(processLocalTransaction *message.SignedTransactionWithHeader, cqc *quorum.LocalTransactionQC) {
	log.LocalDebugf("[%v %v] (processCertificateLocalCqc) Start processCertificateLocalCqc hash %v", pb.ID(), pb.Shard(), processLocalTransaction.Transaction.Transaction.Hash)
	// if processLocalTransaction.Transaction.Transaction.TXType == types.TRANSFER {
	// 	pb.deleteTransferLockTable(processLocalTransaction.Transaction.Transaction.Transaction, processLocalTransaction.Transaction.Transaction.Hash)
	// } else if processLocalTransaction.Transaction.Transaction.TXType == types.SMARTCONTRACT {
	// 	pb.deleteLockTable(processLocalTransaction.Transaction.Transaction.RwSet, processLocalTransaction.Transaction.Transaction.Hash)
	// }

	lt := pb.agreeingLocalTransactions[cqc.TransactionHash][cqc.Nonce].Transaction.Transaction
	if !cqc.IsCommit {
		if lt.TXType == types.TRANSFER {
			pb.deleteTransferLockTable(lt, lt.Hash)
		} else if lt.TXType == types.SMARTCONTRACT {
			pb.deleteLockTable(lt.RwSet, lt.Hash)
		}
	}

	// If Leader, Accept Message Broadcast To Validator
	if cqc.Leader == pb.ID() {
		accept := &message.TransactionAccept{
			SignedTransactionWithHeader: message.SignedTransactionWithHeader{
				Header:      pb.agreeingLocalTransactions[cqc.TransactionHash][cqc.Nonce].Header,
				Transaction: pb.agreeingLocalTransactions[cqc.TransactionHash][cqc.Nonce].Transaction,
			},
		}
		pb.localmu.Unlock()
		if cqc.IsCommit {
			accept.IsCommit = true
		} else {
			accept.IsCommit = false
		}
		validators := pb.FindValidatorsFor(cqc.Epoch)
		pb.BroadcastToSome(validators, accept)
		var vote_complete_transaction message.CompleteTransaction
		vote_complete_transaction.Transaction = accept.Transaction.Transaction
		if cqc.IsCommit {
			vote_complete_transaction.IsCommit = true
		} else {
			vote_complete_transaction.IsCommit = false
		}
		vote_complete_transaction.AbortAndRetryLatencyDissection.ConsensusTime = time.Now().UnixMilli() - vote_complete_transaction.AbortAndRetryLatencyDissection.ConsensusTime
		vote_complete_transaction.LocalLatencyDissection.ConsensusTime = vote_complete_transaction.LocalLatencyDissection.ConsensusTime + vote_complete_transaction.AbortAndRetryLatencyDissection.ConsensusTime
		vote_complete_transaction.AbortAndRetryLatencyDissection.BlockWaitingTime = time.Now().UnixMilli()
		if vote_complete_transaction.IsCommit {
			vote_complete_transaction.LocalLatencyDissection.ProcessTime = time.Now().UnixMilli() - vote_complete_transaction.StartTime
			pb.committed_transactions.AddTxn(&vote_complete_transaction.Transaction)
			log.LocalDebugf("[%v %v] (processCertificateLocalCqc) Add completed consensus Transaction Tx Hash: %v, IsCommit: %v", pb.ID(), pb.Shard(), cqc.TransactionHash, vote_complete_transaction.IsCommit)
		} else {
			go pb.WaitTransactionAddLock(&vote_complete_transaction.Transaction, false)
		}
		pb.SubConsensusTx(cqc.TransactionHash)
	} else {
		pb.localmu.Unlock()
	}
}
