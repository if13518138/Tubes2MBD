// Author: Kun Ren (kun.ren@yale.edu)
// Modified by Daniel Abadi

#include "txn/mvcc_storage.h"
#include <stdio.h>
using namespace std;

// Init the storage
void MVCCStorage::InitStorage() {
  for (int i = 0; i < 1000000;i++) {
    Write(i, 0, 0);
    Mutex* key_mutex = new Mutex();
    mutexs_[i] = key_mutex;
  }
}

// Free memory.
MVCCStorage::~MVCCStorage() {
  for (unordered_map<Key, deque<Version*>*>::iterator it = mvcc_data_.begin();
       it != mvcc_data_.end(); ++it) {
    delete it->second;          
  }
  
  mvcc_data_.clear();
  
  for (unordered_map<Key, Mutex*>::iterator it = mutexs_.begin();
       it != mutexs_.end(); ++it) {
    delete it->second;          
  }
  
  mutexs_.clear();
}

// Lock the key to protect its version_list. Remember to lock the key when you read/update the version_list 
void MVCCStorage::Lock(Key key) {
  if(DEBUG){
    cout << "Lock " << key << endl;
  }
  mutexs_[key]->Lock();
}

// Unlock the key.
void MVCCStorage::Unlock(Key key) {
  if(DEBUG){
    cout << "Unlock " << key << endl << endl;
  }
  mutexs_[key]->Unlock();
}

// MVCC Read
bool MVCCStorage::Read(Key key, Value* result, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Iterate the version_lists and return the verion whose write timestamp
  // (version_id) is the largest write timestamp less than or equal to txn_unique_id.

  // Read always true except key is empty

  if(mvcc_data_.count(key)){
    int MaxVersion = -1;
    // Deque version list
    deque<Version*> *v_list = mvcc_data_[key];
    // Iterate version list
    for(deque<Version*>::iterator it = v_list->begin(); it != v_list->end(); it++){
      int v_wts = (*it)->version_id_;
      if((v_wts > MaxVersion) && (v_wts <= txn_unique_id)){
        MaxVersion = v_wts;
        *result = (*it)->value_;
        if(txn_unique_id > (*it)->max_read_id_){
          (*it)->max_read_id_ = txn_unique_id;
        }
        if(DEBUG){
          cout << "version_id : " << v_wts << endl;
          cout << "max_read_id : " << MaxVersion << endl;
          cout << "txn_unique_id : " << txn_unique_id << endl;
        }
      }
    }
  }else{
    // Key empty
    return false;
  }
  
  return true;
}


// Check whether apply or abort the write
bool MVCCStorage::CheckWrite(Key key, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Before all writes are applied, we need to make sure that each write
  // can be safely applied based on MVCC timestamp ordering protocol. This method
  // only checks one key, so you should call this method for each key in the
  // write_set. Return true if this key passes the check, return false if not. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.

  // Write true iff transaction > largest rts and wts
  if(DEBUG){
    cout << "Currently checking write " << txn_unique_id << endl;
  }
  if(mvcc_data_.count(key)){
    // Deque version list
    deque<Version*> *v_list = mvcc_data_[key];
    // Iterate version list
    int MaxVersion = -1;
    int MaxRTS = -1;
    for(deque<Version*>::iterator it = v_list->begin(); it != v_list->end(); it++){
      // If no current version
      if((*it) == NULL){
        if(MaxVersion > txn_unique_id || MaxRTS > txn_unique_id){
          return false;
        }
      }else{
        if(((*it)->version_id_ > MaxVersion) && ((*it)->version_id_ <= txn_unique_id)){
          MaxVersion = (*it)->version_id_;
          MaxRTS = (*it)->max_read_id_;
        }
      }
    }
    // Result valid iff Ti > max (RTS and WTS)
    return MaxVersion <= txn_unique_id && MaxRTS <= txn_unique_id;
  }else{
    // Key empty
    return false;
  }
}

// MVCC Write, call this method only if CheckWrite return true.
void MVCCStorage::Write(Key key, Value value, int txn_unique_id) {
  // CPSC 438/538:
  //
  // Implement this method!
  
  // Hint: Insert a new version (malloc a Version and specify its value/version_id/max_read_id)
  // into the version_lists. Note that InitStorage() also calls this method to init storage. 
  // Note that you don't have to call Lock(key) in this method, just
  // call Lock(key) before you call this method and call Unlock(key) afterward.
  // Write version
  Version *nVersion = new Version();
  nVersion->value_ = value;
  nVersion->max_read_id_ = txn_unique_id;
  nVersion->version_id_ = txn_unique_id;
  // if(DEBUG){
  //   cout << "version_id " << nVersion->version_id_ << endl;
  //   cout << "max_read_id : " << txn_unique_id << endl;
  //   cout << "txn_unique_id : " << txn_unique_id << endl;
  // }
  // Check if storage exist
  if(!mvcc_data_.count(key)){
    mvcc_data_[key] = new deque<Version*>();
  }
  mvcc_data_[key]->push_back(nVersion);
}


