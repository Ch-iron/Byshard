package pbft

import (
	"unishard/config"
	"unishard/message"
	"unishard/types"
	"unishard/utils"

	"github.com/ethereum/go-ethereum/common"
)

func (pb *PBFT) addLockTable(Rwset []message.RwVariable, transaction_hash common.Hash) bool {
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	lock_false_cnt := 0
	pb.locktablemu.Lock()
	for _, sub_tx := range Rwset {
		if !pb.IsExist(sub_tx.Address) {
			if lock_degree == 3 {
				for _, read_variable := range sub_tx.ReadSet {
					lock_obj := types.Lock{
						TransactionHash: transaction_hash,
						LockType:        types.READ,
						IsAcquire:       true,
					}
					if pb.lockTable_variable[sub_tx.Address] == nil {
						pb.lockTable_variable[sub_tx.Address] = make(map[common.Hash][]*types.Lock)
					}
					pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)] = append(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)], &lock_obj)
					pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
					if len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)]) > 1 {
						if pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-2].LockType == types.WRITE && pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-2].TransactionHash != pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-1].TransactionHash {
							pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-1].IsAcquire = false
							lock_false_cnt++
						}
						if !pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-2].IsAcquire {
							pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(read_variable)])-1].IsAcquire = false
							lock_false_cnt++
						}
					}
				}
			}
			if lock_degree == 3 || lock_degree == 1 {
				for _, write_variable := range sub_tx.WriteSet {
					lock_obj := types.Lock{
						TransactionHash: transaction_hash,
						LockType:        types.WRITE,
						IsAcquire:       true,
					}
					if pb.lockTable_variable[sub_tx.Address] == nil {
						pb.lockTable_variable[sub_tx.Address] = make(map[common.Hash][]*types.Lock)
					}
					pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)] = append(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)], &lock_obj)
					pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
					if len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)]) > 1 {
						if pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)])-2].TransactionHash != pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)])-1].TransactionHash {
							pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)])-1].IsAcquire = false
							lock_false_cnt++
						}
						if !pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)])-2].IsAcquire {
							pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)][len(pb.lockTable_variable[sub_tx.Address][common.HexToHash(write_variable)])-1].IsAcquire = false
							lock_false_cnt++
						}
					}
				}
			}
		}
	}
	if lock_false_cnt > 0 {
		pb.locktablemu.Unlock()
		pb.deleteLockTable(Rwset, transaction_hash)
		return false
	}
	pb.locktablemu.Unlock()
	return true
}

func (pb *PBFT) addTransferLockTable(transfer_transaction message.Transaction, transaction_hash common.Hash) bool {
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	lock_false_cnt := 0

	pb.locktablemu.Lock()
	target_address := []common.Address{}
	target_address = append(target_address, transfer_transaction.From)
	target_address = append(target_address, transfer_transaction.To)
	for _, address := range target_address {
		// if idx == 0 {
		// 	log.LockDebugf("(AddLockTable) From Address: %v", address)
		// } else {
		// 	log.LockDebugf("(AddLockTable) To Address: %v", address)
		// }
		if pb.lockTable_variable[address] == nil {
			pb.lockTable_variable[address] = make(map[common.Hash][]*types.Lock)
		}
		if lock_degree == 3 {
			lock_obj := types.Lock{
				TransactionHash: transaction_hash,
				LockType:        types.READ,
				IsAcquire:       true,
			}
			pb.lockTable_variable[address][utils.SlotToKey(0)] = append(pb.lockTable_variable[address][utils.SlotToKey(0)], &lock_obj)
			pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
			if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) > 1 {
				if pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].LockType == types.WRITE && pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].TransactionHash {
					pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
					lock_false_cnt++
				}
				if !pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].IsAcquire {
					pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
					lock_false_cnt++
				}
			}
		}
		if lock_degree == 3 || lock_degree == 1 {
			lock_obj := types.Lock{
				TransactionHash: transaction_hash,
				LockType:        types.WRITE,
				IsAcquire:       true,
			}
			pb.lockTable_variable[address][utils.SlotToKey(0)] = append(pb.lockTable_variable[address][utils.SlotToKey(0)], &lock_obj)
			pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
			if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) > 1 && pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].TransactionHash {
				pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
				lock_false_cnt++
			}
			if !pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].IsAcquire {
				pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
				lock_false_cnt++
			}
		}
		// for tx_hash, table := range pb.lockTable_transaction {
		// 	log.LockDebugf("(AddLockTable) Tx Hash: %v, Lock Table: %v", tx_hash, &table)
		// }
		// for addr, table := range pb.lockTable_variable {
		// 	log.LockDebugf("(AddLockTable) Address: %v, Lock Table: %v", addr, table[utils.SlotToKey(0)])
		// }
	}
	if lock_false_cnt > 0 {
		pb.locktablemu.Unlock()
		pb.deleteTransferLockTable(transfer_transaction, transaction_hash)
		return false
	}
	pb.locktablemu.Unlock()
	return true
}

func (pb *PBFT) addAddressLockTable(address common.Address, transaction_hash common.Hash) bool {
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	lock_false_cnt := 0

	pb.locktablemu.Lock()
	if pb.lockTable_variable[address] == nil {
		pb.lockTable_variable[address] = make(map[common.Hash][]*types.Lock)
	}
	if lock_degree == 3 {
		lock_obj := types.Lock{
			TransactionHash: transaction_hash,
			LockType:        types.READ,
			IsAcquire:       true,
		}
		pb.lockTable_variable[address][utils.SlotToKey(0)] = append(pb.lockTable_variable[address][utils.SlotToKey(0)], &lock_obj)
		pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
		if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) > 1 {
			if pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].LockType == types.WRITE && pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].TransactionHash {
				pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
				lock_false_cnt++
			}
			if !pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].IsAcquire {
				pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
				lock_false_cnt++
			}
		}
	}
	if lock_degree == 3 || lock_degree == 1 {
		lock_obj := types.Lock{
			TransactionHash: transaction_hash,
			LockType:        types.WRITE,
			IsAcquire:       true,
		}
		pb.lockTable_variable[address][utils.SlotToKey(0)] = append(pb.lockTable_variable[address][utils.SlotToKey(0)], &lock_obj)
		pb.lockTable_transaction[transaction_hash] = append(pb.lockTable_transaction[transaction_hash], &lock_obj)
		if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) > 1 && pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].TransactionHash {
			pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
			lock_false_cnt++
		}
		if !pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-2].IsAcquire {
			pb.lockTable_variable[address][utils.SlotToKey(0)][len(pb.lockTable_variable[address][utils.SlotToKey(0)])-1].IsAcquire = false
			lock_false_cnt++
		}
	}
	if lock_false_cnt > 0 {
		// 락 잡는데에 실패하면 잡았던 락 모두 반환
		pb.locktablemu.Unlock()
		pb.deleteAddressLockTable(address, transaction_hash)
		return false
	}
	pb.locktablemu.Unlock()
	return true
}

// Lock Wake up not use

func (pb *PBFT) deleteLockTable(Rwset []message.RwVariable, transaction_hash common.Hash) {
	// log.LockDebugf("[%v %v] (deleteLockTable) Start deleteLockTable Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	pb.locktablemu.Lock()
	if len(pb.lockTable_transaction[transaction_hash]) == 0 {
		// log.LockDebugf("[%v %v] (deleteLockTable) Finish Zero Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
		pb.locktablemu.Unlock()
		return
	}

	for _, rwset := range Rwset {
		if !pb.IsExist(rwset.Address) {
			if lock_degree == 3 {
				for _, read_variable := range rwset.ReadSet {
					var index int
					for i, lock_obj := range pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)] {
						if lock_obj.TransactionHash == transaction_hash {
							index = i
							break
						}
					}
					// // lock wake up
					// next_transaction := common.Hash{}
					// isNext := 0
					// for i := index + 1; i < len(pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)]); i++ {
					// 	if isNext > 0 {
					// 		if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].TransactionHash == next_transaction {
					// 			pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].IsAcquire = true
					// 			continue
					// 		} else {
					// 			if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i-1].LockType == types.WRITE {
					// 				break
					// 			} else if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i-1].LockType == types.READ {
					// 				if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].LockType == types.WRITE {
					// 					break
					// 				} else if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].LockType == types.READ {
					// 					pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].IsAcquire = true
					// 					continue
					// 				}
					// 			}
					// 		}
					// 	}
					// 	if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].TransactionHash != pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][index].TransactionHash {
					// 		next_transaction = pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].TransactionHash
					// 		isNext++
					// 		pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)][i].IsAcquire = true
					// 	}
					// }
					// log.Errorf("Lock Index: %v, Transaction Hash: %v", index, transaction_hash)
					if pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)] == nil {
						continue
					}
					pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)])
					if len(pb.lockTable_variable[rwset.Address][common.HexToHash(read_variable)]) == 0 {
						delete(pb.lockTable_variable[rwset.Address], common.HexToHash(read_variable))
					}
				}
				if len(pb.lockTable_variable[rwset.Address]) == 0 {
					delete(pb.lockTable_variable, rwset.Address)
				}
			}
			if lock_degree == 3 || lock_degree == 1 {
				for _, write_variable := range rwset.WriteSet {
					var index int
					for i, lock_obj := range pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)] {
						if lock_obj.TransactionHash == transaction_hash {
							index = i
							break
						}
					}
					// lock wake up
					// next_transaction := common.Hash{}
					// isNext := 0
					// for i := index + 1; i < len(pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)]); i++ {
					// 	if isNext > 0 {
					// 		if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].TransactionHash == next_transaction {
					// 			pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].IsAcquire = true
					// 			continue
					// 		} else {
					// 			if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i-1].LockType == types.WRITE {
					// 				break
					// 			} else if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i-1].LockType == types.READ {
					// 				if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].LockType == types.WRITE {
					// 					break
					// 				} else if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].LockType == types.READ {
					// 					pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].IsAcquire = true
					// 					continue
					// 				}
					// 			}
					// 		}
					// 	}
					// 	if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].TransactionHash != pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][index].TransactionHash {
					// 		next_transaction = pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].TransactionHash
					// 		isNext++
					// 		pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)][i].IsAcquire = true
					// 	}
					// }
					if pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)] == nil {
						continue
					}
					pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)])
					if len(pb.lockTable_variable[rwset.Address][common.HexToHash(write_variable)]) == 0 {
						delete(pb.lockTable_variable[rwset.Address], common.HexToHash(write_variable))
					}
				}
				if len(pb.lockTable_variable[rwset.Address]) == 0 {
					delete(pb.lockTable_variable, rwset.Address)
				}
			}
		}
	}
	delete(pb.lockTable_transaction, transaction_hash)
	// log.LockDebugf("[%v %v] (deleteLockTable) Finish Real Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	pb.locktablemu.Unlock()
}

func (pb *PBFT) deleteTransferLockTable(transfer_transaction message.Transaction, transaction_hash common.Hash) {
	// log.LockDebugf("[%v %v] (deleteTransferLockTable) Start deleteLockTable Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	pb.locktablemu.Lock()
	if len(pb.lockTable_transaction[transaction_hash]) == 0 {
		// log.LockDebugf("[%v %v] (deleteTransferLockTable) Finish Zero Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
		pb.locktablemu.Unlock()
		return
	}

	target_address := []common.Address{}
	target_address = append(target_address, transfer_transaction.From)
	target_address = append(target_address, transfer_transaction.To)
	for _, address := range target_address {
		var index int
		for i, lock_obj := range pb.lockTable_variable[address][utils.SlotToKey(0)] {
			if lock_obj.TransactionHash == transaction_hash {
				index = i
				break
			}
		}

		// lock wake up
		// next_transaction := common.Hash{}
		// isNext := 0
		// for i := index + 2; i < len(pb.lockTable_variable[address][utils.SlotToKey(0)]); i++ {
		// 	if isNext > 0 {
		// 		if pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash == next_transaction {
		// 			pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
		// 			continue
		// 		} else {
		// 			if pb.lockTable_variable[address][utils.SlotToKey(0)][i-1].LockType == types.WRITE {
		// 				break
		// 			} else if pb.lockTable_variable[address][utils.SlotToKey(0)][i-1].LockType == types.READ {
		// 				if pb.lockTable_variable[address][utils.SlotToKey(0)][i].LockType == types.WRITE {
		// 					break
		// 				} else if pb.lockTable_variable[address][utils.SlotToKey(0)][i].LockType == types.READ {
		// 					pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
		// 					continue
		// 				}
		// 			}
		// 		}
		// 	}
		// 	if pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][index].TransactionHash {
		// 		next_transaction = pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash
		// 		isNext++
		// 		pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
		// 	}
		// }
		if pb.lockTable_variable[address][utils.SlotToKey(0)] == nil {
			continue
		}
		if lock_degree == 3 {
			pb.lockTable_variable[address][utils.SlotToKey(0)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[address][utils.SlotToKey(0)])
		}
		if lock_degree == 3 || lock_degree == 1 {
			pb.lockTable_variable[address][utils.SlotToKey(0)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[address][utils.SlotToKey(0)])
		}
		if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) == 0 {
			delete(pb.lockTable_variable[address], utils.SlotToKey(0))
		}
		if len(pb.lockTable_variable[address]) == 0 {
			delete(pb.lockTable_variable, address)
		}
		delete(pb.lockTable_transaction, transaction_hash)
	}
	// log.LockDebugf("[%v %v] (deleteTransferLockTable) Finish Real Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	pb.locktablemu.Unlock()
}

func (pb *PBFT) deleteAddressLockTable(address common.Address, transaction_hash common.Hash) {
	// log.LockDebugf("[%v %v] (deleteAddressLockTable) Start deleteLockTable Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	var lock_degree = config.GetConfig().LockingDegree
	// var mutex = &sync.Mutex{}

	pb.locktablemu.Lock()
	if len(pb.lockTable_transaction[transaction_hash]) == 0 {
		// log.LockDebugf("[%v %v] (deleteAddressLockTable) Finish Zero Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
		pb.locktablemu.Unlock()
		return
	}

	if pb.lockTable_variable[address][utils.SlotToKey(0)] == nil {
		pb.locktablemu.Unlock()
		return
	}

	var index int
	for i, lock_obj := range pb.lockTable_variable[address][utils.SlotToKey(0)] {
		if lock_obj.TransactionHash == transaction_hash {
			index = i
			break
		}
	}

	// lock wake up
	// next_transaction := common.Hash{}
	// isNext := 0
	// for i := index + 2; i < len(pb.lockTable_variable[address][utils.SlotToKey(0)]); i++ {
	// 	if isNext > 0 {
	// 		if pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash == next_transaction {
	// 			pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
	// 			continue
	// 		} else {
	// 			if pb.lockTable_variable[address][utils.SlotToKey(0)][i-1].LockType == types.WRITE {
	// 				break
	// 			} else if pb.lockTable_variable[address][utils.SlotToKey(0)][i-1].LockType == types.READ {
	// 				if pb.lockTable_variable[address][utils.SlotToKey(0)][i].LockType == types.WRITE {
	// 					break
	// 				} else if pb.lockTable_variable[address][utils.SlotToKey(0)][i].LockType == types.READ {
	// 					pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
	// 					continue
	// 				}
	// 			}
	// 		}
	// 	}
	// 	if pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash != pb.lockTable_variable[address][utils.SlotToKey(0)][index].TransactionHash {
	// 		next_transaction = pb.lockTable_variable[address][utils.SlotToKey(0)][i].TransactionHash
	// 		isNext++
	// 		pb.lockTable_variable[address][utils.SlotToKey(0)][i].IsAcquire = true
	// 	}
	// }
	if lock_degree == 3 {
		pb.lockTable_variable[address][utils.SlotToKey(0)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[address][utils.SlotToKey(0)])
	}
	if lock_degree == 3 || lock_degree == 1 {
		pb.lockTable_variable[address][utils.SlotToKey(0)] = utils.RemoveSliceIndex(index, pb.lockTable_variable[address][utils.SlotToKey(0)])
	}
	if len(pb.lockTable_variable[address][utils.SlotToKey(0)]) == 0 {
		delete(pb.lockTable_variable[address], utils.SlotToKey(0))
	}
	if len(pb.lockTable_variable[address]) == 0 {
		delete(pb.lockTable_variable, address)
	}
	delete(pb.lockTable_transaction, transaction_hash)
	// log.LockDebugf("[%v %v] (deleteAddressLockTable) Finish Real Hash: %v", pb.ID(), pb.Shard(), transaction_hash)
	pb.locktablemu.Unlock()
}
