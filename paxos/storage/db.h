/*
 * Module_name: db.h
 * Author: Barneyliu
 * Time: 2018-12-19
 * Description:
 *
 */
 
#pragma once

#include <vector>
#include <string>
#include <map>
#include "comm_include.h"
#include "storage.h"
#include "log_store.h"
#include "config_store.h"

namespace multipaxos
{

class Database
{
public:
  Database();
  virtual ~Database();

  int Init(const std::string & sDBPath, const int iMyGroupIdx);
  const std::string GetDBPath();
  int ClearAllLog();
  int Get(const uint64_t llInstanceID, std::string & sValue);
  int Put(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sValue);
  int Del(const WriteOptions & oWriteOptions, const uint64_t llInstanceID);
  int ForceDel(const WriteOptions & oWriteOptions, const uint64_t llInstanceID);
  int GetMaxInstanceID(uint64_t & llInstanceID);
  int SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const uint64_t llMinInstanceID);
  int GetMinChosenInstanceID(uint64_t & llMinInstanceID);
  int SetSystemVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer);
  int GetSystemVariables(std::string & sBuffer);
  int SetMasterVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer);
  int GetMasterVariables(std::string & sBuffer);
    
public:
  int GetMaxInstanceIDFileID(std::string & sFileID, uint64_t & llInstanceID);
  //int RebuildOneIndex(const uint64_t llInstanceID, const std::string & sFileID);
    
private:
  int ValueToFileID(const WriteOptions & oWriteOptions, const uint64_t llInstanceID,
		    const std::string & sValue, std::string & sFileID);

  int FileIDToValue(const std::string & sFileID, uint64_t & llInstanceID, std::string & sValue);
        
private:
  std::string GenKey(const uint64_t llInstanceID);
  const uint64_t GetInstanceIDFromKey(const std::string & sKey);

public:
//private:
  bool m_bHasInit;
  LogStore * m_poValueStore;
  ConfigStore *m_sysvarStore;
  ConfigStore *m_masterStore;
  ConfigStore *m_minchosenStore;
  
  std::string m_sDBPath;
  int m_iMyGroupIdx;
  ardb::Buffer m_oTmpBuffer;

private:
  TimeStat m_oTimeStat;
};

//////////////////////////////////////////

class MultiDatabase : public LogStorage
{
public:
  MultiDatabase();
  virtual ~MultiDatabase();

  int Init(const std::string & sDBPath, const int iGroupCount);
  const std::string GetLogStorageDirPath(const int iGroupIdx);
  int Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue);
  int Put(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID,
	  const std::string & sValue);
  int Del(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID);
  int ForceDel(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID);
  int GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID);
  int SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llMinInstanceID);
  int GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID);
  int ClearAllLog(const int iGroupIdx);
  int SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer);
  int GetSystemVariables(const int iGroupIdx, std::string & sBuffer);
  int SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx, const std::string & sBuffer);
  int GetMasterVariables(const int iGroupIdx, std::string & sBuffer);

private:
  std::vector<Database *> m_vecDBList;
};

}

