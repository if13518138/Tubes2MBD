// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include <deque>

#include "txn/lock_manager.h"

using std::deque;

LockManager::~LockManager() {
  // Cleanup lock_table_
  for (auto element = lock_table_.begin(); element != lock_table_.end(); element++) {
    delete element->second;
  }
}

deque<LockManager::LockRequest>* LockManager::_getLockQueue(const Key& key) {
  // Get queue in lockTable
  deque<LockRequest> *lock_queue = lock_table_[key];
  if (!lock_queue) {
    // Define new queue and assign to lock table
    lock_queue = new deque<LockRequest>();
    lock_table_[key] = lock_queue;
  }
  return lock_queue;
}

LockManagerA::LockManagerA(deque<Txn*>* ready_txns) {
  ready_txns_ = ready_txns;
}

bool LockManagerA::WriteLock(Txn* txn, const Key& key) {
  bool is_empty = true;
  // Create Request
  LockRequest lock_request(EXCLUSIVE, txn);
  deque<LockRequest> *lock_queue = _getLockQueue(key);
  // Check if lock exist
  is_empty = lock_queue->empty();
  // Add request to queue
  lock_queue->push_back(lock_request);

  if (!is_empty) { 
    // Add to wait list, doesn't own lock immediately.
    txn_waits_[txn]++;
  }
  return is_empty;
}

bool LockManagerA::ReadLock(Txn* txn, const Key& key) {
  // Same cuz EXCLUSIVE ONLY Boyss
  return WriteLock(txn, key);
}

void LockManagerA::Release(Txn* txn, const Key& key) {
  deque<LockRequest> *lock_queue = _getLockQueue(key);
  bool removedOwner = true;

  // Delete the txn's exclusive lock.
  for (auto element = lock_queue->begin(); element < lock_queue->end(); element++) {
    if (element->txn_ == txn) {
        lock_queue->erase(element);
        break;
    }
    removedOwner = false;
  }

  if (!lock_queue->empty() && removedOwner) {
    // Grantt the next transaction the lock
    LockRequest nextReq = lock_queue->front();

    if (--txn_waits_[nextReq.txn_] == 0) {
      // push to ready and erase at waits the req
        ready_txns_->push_back(nextReq.txn_);
        txn_waits_.erase(nextReq.txn_);
    }
  }
}

LockMode LockManagerA::Status(const Key& key, vector<Txn*>* owners) {
  deque<LockRequest> *dq = _getLockQueue(key);
  if (dq->empty()) {
    return UNLOCKED;
  } else {
    vector<Txn*> _owners;
    _owners.push_back(dq->front().txn_);
    *owners = _owners;
    return EXCLUSIVE;
  }
}