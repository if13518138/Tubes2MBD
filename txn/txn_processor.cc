// Author: Alexander Thomson (thomson@cs.yale.edu)
// Modified by: Christina Wallin (christina.wallin@yale.edu)
// Modified by: Kun Ren (kun.ren@yale.edu)
// Modified by : Lionnarta Savirandy (13518128)


#include "txn/txn_processor.h"
#include <stdio.h>
#include <set>

#include "txn/lock_manager.h"

using namespace std;

// Thread & queue counts for StaticThreadPool initialization.
#define THREAD_COUNT 8

TxnProcessor::TxnProcessor(CCMode mode)
    : mode_(mode), tp_(THREAD_COUNT), next_unique_id_(1) {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY)
    lm_ = new LockManagerA(&ready_txns_);

  // Create the storage
  if (mode_ == MVCC) {
    storage_ = new MVCCStorage();
  } else {
    storage_ = new Storage();
  }

  storage_->InitStorage();

  // Start 'RunScheduler()' running.
  cpu_set_t cpuset;
  pthread_attr_t attr;
  pthread_attr_init(&attr);
  CPU_ZERO(&cpuset);
  CPU_SET(0, &cpuset);
  CPU_SET(1, &cpuset);
  CPU_SET(2, &cpuset);
  CPU_SET(3, &cpuset);
  CPU_SET(4, &cpuset);
  CPU_SET(5, &cpuset);
  CPU_SET(6, &cpuset);
  pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
  pthread_t scheduler_;
  pthread_create(&scheduler_, &attr, StartScheduler, reinterpret_cast<void*>(this));

}

void* TxnProcessor::StartScheduler(void * arg) {
  reinterpret_cast<TxnProcessor *>(arg)->RunScheduler();
  return NULL;
}

TxnProcessor::~TxnProcessor() {
  if (mode_ == LOCKING_EXCLUSIVE_ONLY || mode_ == LOCKING)
    delete lm_;

  delete storage_;
}

void TxnProcessor::NewTxnRequest(Txn* txn) {
  // Atomically assign the txn a new number and add it to the incoming txn
  // requests queue.
  mutex_.Lock();
  txn->unique_id_ = next_unique_id_;
  next_unique_id_++;
  txn_requests_.Push(txn);
  mutex_.Unlock();
}

Txn* TxnProcessor::GetTxnResult() {
  Txn* txn;
  while (!txn_results_.Pop(&txn)) {
    // No result yet. Wait a bit before trying again (to reduce contention on
    // atomic queues).
    sleep(0.000001);
  }
  return txn;
}

void TxnProcessor::RunScheduler() {
  switch (mode_) {
    case SERIAL:                 RunSerialScheduler(); break;
    case LOCKING:                RunLockingScheduler(); break;
    case LOCKING_EXCLUSIVE_ONLY: RunLockingScheduler(); break;
    case OCC:                    RunOCCScheduler(); break;
    case P_OCC:                  RunOCCParallelScheduler(); break;
    case MVCC:                   RunMVCCScheduler();
  }
}

void TxnProcessor::RunSerialScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Get next txn request.
    if (txn_requests_.Pop(&txn)) {
      // Execute txn.
      ExecuteTxn(txn);

      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Return result to client.
      txn_results_.Push(txn);
    }
  }
}

void TxnProcessor::RunLockingScheduler() {
  Txn* txn;
  while (tp_.Active()) {
    // Start processing the next incoming transaction request.
    if (txn_requests_.Pop(&txn)) {
      if(DEBUG) {
        cout << endl;
        cout << "Currently checking " << txn->unique_id_ << endl;
      }
      bool blocked = false;
      // Request read locks.
      if(DEBUG) {
        cout << "Transaction " << txn->unique_id_ << " Requesting Read Lock" << endl;
      }
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        if (!lm_->ReadLock(txn, *it)) {
          blocked = true;
          // If readset_.size() + writeset_.size() > 1, and blocked, just abort
          if (txn->readset_.size() + txn->writeset_.size() > 1) {
            // Release all locks that already acquired
            if(DEBUG) {cout << "Aborting Action" << endl;}
            for (set<Key>::iterator it_reads = txn->readset_.begin(); true; ++it_reads) {
              lm_->Release(txn, *it_reads);
              if (it_reads == it) {
                break;
              }
            }
            break;
          }
        }
      }

      if (blocked == false) {
        // Request write locks.
        if(DEBUG) {cout << "Transaction" << txn->unique_id_ << "Requesting Read Lock" << endl;}
        for (set<Key>::iterator it = txn->writeset_.begin();
             it != txn->writeset_.end(); ++it) {
          if (!lm_->WriteLock(txn, *it)) {
            blocked = true;
            // If readset_.size() + writeset_.size() > 1, and blocked, just abort
            if (txn->readset_.size() + txn->writeset_.size() > 1) {
              // Release all read locks that already acquired
              if (DEBUG) {cout << "Aborting Action" << endl;}
              for (set<Key>::iterator it_reads = txn->readset_.begin(); it_reads != txn->readset_.end(); ++it_reads) {
                lm_->Release(txn, *it_reads);
              }
              // Release all write locks that already acquired
              for (set<Key>::iterator it_writes = txn->writeset_.begin(); true; ++it_writes) {
                lm_->Release(txn, *it_writes);
                if (it_writes == it) {
                  break;
                }
              }
              break;
            }
          }
        }
      }

      // If all read and write locks were immediately acquired, this txn is
      // ready to be executed. Else, just restart the txn
      if (blocked == false) {
        if(DEBUG) {cout << "Read or Write Lock immediately acquired, Transaction " << txn->unique_id_ << " Ready to Be Executed" << endl;}
        ready_txns_.push_back(txn);
      } else if (blocked == true && (txn->writeset_.size() + txn->readset_.size() > 1)){
        if(DEBUG) {cout << "Read or Write Lock Can't immediately acquired, Rebooting Transaction" << txn->unique_id_ << endl;}
        mutex_.Lock();
        txn->unique_id_ = next_unique_id_;
        next_unique_id_++;
        txn_requests_.Push(txn);
        mutex_.Unlock();
      }
    }

    // Process and commit all transactions that have finished running.
    while (completed_txns_.Pop(&txn)) {
      // Commit/abort txn according to program logic's commit/abort decision.
      if (txn->Status() == COMPLETED_C) {
        if (DEBUG) {cout << "Transaction" << txn->unique_id_ << " Commited" << endl;}
        ApplyWrites(txn);
        txn->status_ = COMMITTED;
      } else if (txn->Status() == COMPLETED_A) {
        if(DEBUG) {cout << "Transaction" << txn->unique_id_ << " Aborted" << endl;}
        txn->status_ = ABORTED;
      } else {
        // Invalid TxnStatus!
        DIE("Completed Txn has invalid TxnStatus: " << txn->Status());
      }

      // Release read locks.
      if(DEBUG) {cout << "Release Read Lock Of Transaction" << txn->unique_id_ << endl;}
      for (set<Key>::iterator it = txn->readset_.begin();
           it != txn->readset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Release write locks.
      if(DEBUG) {cout << "Release write Lock Of Transaction" << txn->unique_id_ << endl;}
      for (set<Key>::iterator it = txn->writeset_.begin();
           it != txn->writeset_.end(); ++it) {
        lm_->Release(txn, *it);
      }
      // Return result to client.
      txn_results_.Push(txn);
    }
    // Start executing all transactions that have newly acquired all their
    // locks.
    while (ready_txns_.size()) {
      // Get next ready txn from the queue.
      txn = ready_txns_.front();
      ready_txns_.pop_front();

      // Start txn running in its own thread.
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(
            this,
            &TxnProcessor::ExecuteTxn,
            txn));

    }
  }
}

void TxnProcessor::ExecuteTxn(Txn* txn) {

  // Get the start time
  txn->occ_start_time_ = GetTime();

  // Read everything in from readset.
  for (set<Key>::iterator it = txn->readset_.begin();
       it != txn->readset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Also read everything in from writeset.
  for (set<Key>::iterator it = txn->writeset_.begin();
       it != txn->writeset_.end(); ++it) {
    // Save each read result iff record exists in storage.
    Value result;
    if (storage_->Read(*it, &result))
      txn->reads_[*it] = result;
  }

  // Execute txn's program logic.
  txn->Run();

  // Hand the txn back to the RunScheduler thread.
  completed_txns_.Push(txn);
}

void TxnProcessor::ApplyWrites(Txn* txn) {
  // Write buffered writes out to storage.
  for (map<Key, Value>::iterator it = txn->writes_.begin();
       it != txn->writes_.end(); ++it) {
    storage_->Write(it->first, it->second, txn->unique_id_);
  }
}

/**
 * Precondition: No storage writes occured during execution.
 */
bool TxnProcessor::OCCValidateTransaction(const Txn &txn) const {
  // No transaction should be allowed to write data when this transaction is still running
  // Which is identified by the timestamp of the data being later then the transaction's start timestamp
  for (auto&& key : txn.readset_) {
    if(DEBUG) {
      cout << "Currently checking " << txn.unique_id_ << endl;
      cout << "Start time : " << txn.occ_start_time_ << endl;
      cout << "Data last write : " << storage_->Timestamp(key) << endl;
    }
    if (txn.occ_start_time_ < storage_->Timestamp(key)){
      if(DEBUG) {cout << "Invalid, data overwritten when transaction is running" << endl;}
      return false;
    }
  }

  for (auto&& key : txn.writeset_) {
    if(DEBUG) {
      cout << "Currently checking " << txn.unique_id_ << endl;
      cout << "Start time : " << txn.occ_start_time_ << endl;
      cout << "Data last write : " << storage_->Timestamp(key) << endl;
    }
    if (txn.occ_start_time_ < storage_->Timestamp(key)){
      if(DEBUG) {cout << "Invalid, data overwritten when transaction is running" << endl;}
      return false;
    }
  }

  return true;
}

void TxnProcessor::RunOCCScheduler() {
  cout << fixed;
  // Loop for continuous checking of running transaction
  while (tp_.Active()) {
    Txn *currentTransaction;
    // Fetch current transaction from the request queue
    if (txn_requests_.Pop(&currentTransaction)) {
      // Start current transactiontransaction in its own thread
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(this, &TxnProcessor::ExecuteTxn, currentTransaction));
    }

    // Start validation of finished transaction
    Txn *finishedTransaction;
    while (completed_txns_.Pop(&finishedTransaction)) {
      if (finishedTransaction->Status() == COMPLETED_A) { 
        // If the completion status is COMPLETED_A, then deem it aborted
        finishedTransaction->status_ = ABORTED;
        if(DEBUG) {cout << "Transaction is abort-voted"<< endl;}
      } else {
        // Check if transaction is valid according to its readset and writeset timestamp
        bool isTransactionValid = OCCValidateTransaction(*finishedTransaction);
        //isTransactionValid = true; //Unidentified segfault, avoided by setting this to true
        if (!isTransactionValid) {
          // Invalid transaction will be cleaned up and restarted
          finishedTransaction->reads_.empty();
          finishedTransaction->writes_.empty();
          finishedTransaction->status_ = INCOMPLETE;

          mutex_.Lock();
          currentTransaction->unique_id_ = next_unique_id_;
          next_unique_id_++;
          txn_requests_.Push(finishedTransaction);
          mutex_.Unlock();
          if(DEBUG) {
            cout << "Transaction is invalid" << endl;
            cout << "Cleaning up and restarting" << endl;
          }
        } else {
          // Valid Transaction will be committed
          ApplyWrites(finishedTransaction);
          currentTransaction->status_ = COMMITTED;
          if(DEBUG){cout << "Transaction is valid" << endl;}
        }
      }

      // The result will be pushed into a list of transaction results
      txn_results_.Push(finishedTransaction);
      if(DEBUG) {cout << endl;}
    }
  }
}

void TxnProcessor::RunOCCParallelScheduler() {
  // CPSC 438/538:
  //
  // Implement this method! Note that implementing OCC with parallel
  // validation may need to create another method, like
  // TxnProcessor::ExecuteTxnParallel.
  // Note that you can use active_set_ and active_set_mutex_ we provided
  // for you in the txn_processor.h
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  RunSerialScheduler();
}

void TxnProcessor::RunMVCCScheduler() {
  // CPSC 438/538:
  //
  // Implement this method!

  // Hint:Pop a txn from txn_requests_, and pass it to a thread to execute.
  // Note that you may need to create another execute method, like TxnProcessor::MVCCExecuteTxn.
  //
  // [For now, run serial scheduler in order to make it through the test
  // suite]
  Txn *txn;
  while(tp_.Active()){
    // Check txn availability
    if(txn_requests_.Pop(&txn)){
      // Thread execute
      tp_.RunTask(new Method<TxnProcessor, void, Txn*>(this, &TxnProcessor::MVCCExecuteTxn, txn));
    }
  }
}

void TxnProcessor::MVCCExecuteTxn(Txn *txn){
  // Based on pseudo code in README on reference
  Value res;
  bool ver = true;
  if(DEBUG){
    cout << "Transaction " << txn->unique_id_ << endl;
  }
  // Read all necessary data for this transaction from storage (Note that you should lock the key before each read)
  // Readset
  for(set<Key>::iterator it = txn->readset_.begin(); it != txn->readset_.end(); it++){
    storage_->Lock(*it);
    if(storage_->Read(*it, &res, txn->unique_id_)){
      txn->reads_[*it] = res;
    }
    storage_->Unlock(*it);
  }
  // Writeset
  for(set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
    storage_->Lock(*it);
    if(storage_->Read(*it, &res, txn->unique_id_)){
      txn->reads_[*it] = res;
    }
    storage_->Unlock(*it);
  }
  // Execute the transaction logic (i.e. call Run() on the transaction)
  txn->Run();
  completed_txns_.Push(txn);
  // Acquire all locks for keys in the write_set_
  for(set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
    // Call MVCCStorage::CheckWrite method to check all keys in the write_set_
    if(!storage_->CheckWrite(*it, txn->unique_id_)){
      ver = false;
      break;
    }
  }
  // If (each key passed the check)
  if(ver){
    // Apply the writes
    ApplyWrites(txn);
    // Release all locks for keys in the write_set_
    for(set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
      storage_->Unlock(*it);
    }
    // Commit transaction
    txn->status_ = COMMITTED;
    txn_results_.Push(txn);
  //   else if (at least one key failed the check)
  }else{
    // Release all locks for keys in the write_set_
    for(set<Key>::iterator it = txn->writeset_.begin(); it != txn->writeset_.end(); it++){
      storage_->Unlock(*it);
    }
    // Cleanup txn
    txn->reads_.empty();
    txn->writes_.empty();
    txn->status_ = INCOMPLETE;
    // Completely restart the transaction.
    mutex_.Lock();
    txn->unique_id_ = next_unique_id_;
    next_unique_id_++;
    txn_requests_.Push(txn);
    mutex_.Unlock(); 
  }
}
