#pragma once

#include <mutex>
#include "rocksdb/db.h"
#include <string>
#include <vector>
#include "async_boost_log.h"
#include "def.h"

namespace phxkv
{

#define KV_CHECKPOINT_KEY ((uint64_t)-1)

class KVClient
{
public:
  KVClient();
  virtual ~KVClient();

  bool Init(const std::string & sDBPath);
  static KVClient * Instance();

  KVClientRet Get(const std::string & sKey, std::string & sValue, uint64_t & llVersion);

  KVClientRet GetChild(const std::string & sKey, std::vector<std::string>& sValue, uint64_t & llVersion);

  KVClientRet Set(const std::string & sKey, const std::string & sValue, const uint64_t llVersion);

  KVClientRet Create(const std::string & sKey, const std::string & sValue, const uint64_t llVersion);

  KVClientRet Del(const std::string & sKey, const uint64_t llVersion);

private:
  rocksdb::DB * m_poRocksDB;
  bool m_bHasInit;
  std::mutex m_oMutex;
};
    
}

