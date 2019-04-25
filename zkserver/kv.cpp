#include "kv.h"
#include "phxkv.pb.h"

using namespace std;

namespace phxkv
{

KVClient :: KVClient(){
  m_bHasInit = false;
  m_poRocksDB = nullptr;
}

KVClient :: ~KVClient(){
}

bool KVClient :: Init(const std::string & sDBPath){
  if (m_bHasInit) {
    return true;
  }

  rocksdb::Options oOptions;
  oOptions.create_if_missing = true;
  rocksdb::Status oStatus = rocksdb::DB::Open(oOptions, sDBPath, &m_poRocksDB);

  if (!oStatus.ok()) {
    ZKLOG_ERROR << "Open rocksdb fail, db_path " << sDBPath;
    return false;
  }

  m_bHasInit = true;
  ZKLOG_INFO << "OK, db_path " << sDBPath;
  return true;
}

KVClient * KVClient :: Instance(){
  static KVClient oKVClient;
  return &oKVClient;
}

KVClientRet KVClient :: Get(const std::string & sKey, std::string & sValue, uint64_t & llVersion){
  if (!m_bHasInit) {
    ZKLOG_ERROR << "no init yet";
    return ZSYSTEMERROR;
  }

  string sBuffer;
  rocksdb::Status oStatus = m_poRocksDB->Get(rocksdb::ReadOptions(), sKey, &sBuffer);
  if (!oStatus.ok()) {
    if (oStatus.IsNotFound()) {
      ZKLOG_ERROR << "RocksDB.Get not found, key " << sKey;
      llVersion = 0;
      return ZNONODE;
    }

    ZKLOG_ERROR << "RocksDB.Get fail, key " << sKey;
    return ZSYSTEMERROR;
  }

  KVData oData;
  bool bSucc = oData.ParseFromArray(sBuffer.data(), sBuffer.size());
  if (!bSucc) {
    ZKLOG_ERROR << "DB DATA wrong, key " << sKey;
    return ZSYSTEMERROR;
  }

  llVersion = oData.version();
  if (oData.isdeleted()) {
    ZKLOG_ERROR << "RocksDB.Get key already deleted, key " << sKey;
    return ZNONODE;
  }

  sValue = oData.value();
  ZKLOG_INFO << "OK, Get key " << sKey << " value " << sValue << " version " << llVersion;
  return ZOK;
}

KVClientRet KVClient :: GetChild(const std::string & sKey, std::vector<std::string>& sValue, uint64_t & llVersion){
  if (!m_bHasInit) {
    ZKLOG_ERROR << "no init yet";
    return ZSYSTEMERROR;
  }

  rocksdb::Iterator *it = m_poRocksDB->NewIterator(rocksdb::ReadOptions());
  it->SeekToFirst();

  while (it->Valid()) {
    std::string skey_path = it->key().ToString();
    std::string parent_skey = skey_path.substr(0, skey_path.rfind("/"));
    std::string skey_name = skey_path.substr(skey_path.rfind("/")+1);

    if(parent_skey == sKey)
      sValue.push_back(skey_name);

    it->Next();
  }

  delete it;
  ZKLOG_INFO << "OK, GetChild key " << sKey;
  return ZOK;
}

KVClientRet KVClient :: Set(const std::string & sKey, const std::string & sValue, const uint64_t llVersion){
  if (!m_bHasInit) {
    ZKLOG_ERROR << "no init yet";
    return ZSYSTEMERROR;
  }

  std::lock_guard<std::mutex> oLockGuard(m_oMutex);
  
  uint64_t llServerVersion = 0;
  std::string sServerValue;
  KVClientRet ret = Get(sKey, sServerValue, llServerVersion);
  if(ret == ZOK) {
    if(llVersion != -1) {
      if (llServerVersion != llVersion) {
	ZKLOG_INFO << "Set key " << sKey << " version " << llServerVersion << " is match, input version "
		   <<  llVersion;
	return ZBADVERSION;
      }
    }

    llServerVersion++;
    KVData oData;
    oData.set_value(sValue);
    oData.set_version(llServerVersion);
    oData.set_isdeleted(false);

    std::string sBuffer;
    bool bSucc = oData.SerializeToString(&sBuffer);
    if (!bSucc) {
      ZKLOG_ERROR << "Data.SerializeToString fail";
      return ZSYSTEMERROR;
    }

    rocksdb::Status oStatus = m_poRocksDB->Put(rocksdb::WriteOptions(), sKey, sBuffer);
    if (!oStatus.ok()) {
      ZKLOG_ERROR << "RocksDB.Put fail, key " << sKey << " bufferlen " << sBuffer.size();
      return ZSYSTEMERROR;
    }

    ZKLOG_INFO << "OK, Set key " << sKey << " value " << sValue << " version " << llServerVersion;
    return ZOK;
  }

  return ret;
}

KVClientRet KVClient :: Create(const std::string & sKey, const std::string & sValue, const uint64_t llVersion){
  if (!m_bHasInit) {
    ZKLOG_ERROR << "no init yet";
    return ZSYSTEMERROR;
  }

  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  uint64_t llServerVersion = 0;
  std::string sServerValue;
  KVClientRet ret = Get(sKey, sServerValue, llServerVersion);
  if(ret == ZNONODE) {
    llServerVersion++;
    KVData oData;
    oData.set_value(sValue);
    oData.set_version(llServerVersion);
    oData.set_isdeleted(false);

    std::string sBuffer;
    bool bSucc = oData.SerializeToString(&sBuffer);
    if (!bSucc) {
      ZKLOG_ERROR << "Data.SerializeToString fail";
      return ZSYSTEMERROR;
    }

    rocksdb::Status oStatus = m_poRocksDB->Put(rocksdb::WriteOptions(), sKey, sBuffer);
    if (!oStatus.ok()) {
      ZKLOG_ERROR << "RocksDB.Put fail, key " << sKey << " bufferlen " << sBuffer.size();
      return ZSYSTEMERROR;
    }

    ZKLOG_INFO << "OK, Create key " << sKey << " value " << sValue << " version " << llVersion;
    return ZOK;
  }else if(ret == ZOK)
    return ZNODEEXISTS;

  return ret;
}


KVClientRet KVClient :: Del(const std::string & sKey, const uint64_t llVersion){
  if (!m_bHasInit) {
    ZKLOG_ERROR << "no init yet";
    return ZSYSTEMERROR;
  }

  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  uint64_t llServerVersion = 0;
  std::string sServerValue;
  KVClientRet ret = Get(sKey, sServerValue, llServerVersion);
  if(ret != ZOK) {
    return ret;
  }

  if(llVersion != -1) {
    if (llServerVersion != llVersion) {
      ZKLOG_INFO << "Set Del " << sKey << " version " << llServerVersion << " is match, input version "
		 << llVersion;
      return ZBADVERSION;
    }
  }

  rocksdb::Status oStatus = m_poRocksDB->Delete(rocksdb::WriteOptions(), sKey);
  if (!oStatus.ok()) {
    ZKLOG_ERROR << "RocksDB.Delete fail, key " << sKey;
    return ZSYSTEMERROR;
  }

  ZKLOG_INFO << "OK, Del key " << sKey << " version " << llServerVersion;
  return ZOK;
}
    
}

