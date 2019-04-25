/*
 * Module_name: Log_store.h
 * Author: Barneyliu
 * Time: 2018-12-26
 * Description:
 *
 */

#pragma once
 
#include <string>
#include <mutex>
#include "buffer.hpp"
#include "util.h"
#include "storage.h"
#include "inside_options.h"
#include <map>
#include "skiplist.h"
#include "arena.h"
 
namespace multipaxos
{
 
class Database;

#define FILEID_LEN (sizeof(int) + sizeof(int) + sizeof(uint32_t))

struct KeyComparator {
  int operator() (const char* a, const char* b) const {
    int ret;
    uint64_t a_llInstanceID, b_llInstanceID;
    memcpy(&a_llInstanceID, a+sizeof(int), sizeof(uint64_t));
    memcpy(&b_llInstanceID, b+sizeof(int), sizeof(uint64_t));
    if(a_llInstanceID < b_llInstanceID)
      ret = -1;
    else if(a_llInstanceID == b_llInstanceID)
      ret = 0;
    else
      ret = 1;

    return ret;
  }
};

typedef leveldb::SkipList<const char*, KeyComparator> Table_;

class MemTable 
{
public:
  MemTable(uint64_t logZxid);
  uint64_t get_logZxid(){
    return m_logZxid;
  }

  Table_& get_inntable() {
    return m_table;
  }

  void Add(const char* sbuf, int sbuf_len);
	
private:
  leveldb::Arena arena_;
  uint64_t m_logZxid;
  Table_ m_table;
};

class LogStoreLogger
{
public:
  LogStoreLogger();
  virtual ~LogStoreLogger();

  void Init(const std::string & sPath);
  void Log(const char * pcFormat, ...);
 
private:
  int m_iLogFd;
};
 
class LogStore
{
public:
  LogStore();
  virtual ~LogStore();

  int Init(const std::string & sPath, const int iMyGroupIdx, int resv_lognum=10);
  int Append(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sBuffer);
  int Read(const uint64_t & llInstanceID, std::string & sBuffer);
  int Del(const uint64_t llInstanceID);
  int ForceDel(const uint64_t llInstanceID);

  ////////////////////////////////////////////
  const bool IsValidFileID(const std::string & sFileID);
  ////////////////////////////////////////////

  int RebuildIndex(const uint64_t iFileID, int& iNowFileWriteOffset);

  //int RebuildIndexForOneFile(const int iFileID, const int iOffset,
  //		 Database * poDatabase, int & iNowFileWriteOffset, uint64_t & llNowInstanceID);

  int GetMaxInstanceIDFileID(std::string & sFileID, uint64_t & llInstanceID);
  int GetFileIDFromInstanceID(std::string & sFileID, const uint64_t& llInstanceID);
	 
 private:
  void GenFileID(const int iFileID, const int iOffset, const uint32_t iCheckSum, std::string & sFileID);
  void ParseFileID(const std::string & sFileID, int & iFileID, int & iOffset, uint32_t & iCheckSum);
  int IncreaseFileID();
  int UtilGetLogFiles(const std::string& prefix);
  int UtilClearLogFile();
  int OpenFile(uint64_t iFileID, int & iFd);
  int DeleteFile(const std::string& filename);
  int GetFileFD(const int iNeedWriteSize, const uint64_t llInstanceID, int & iFd, int & iOffset);
  int ExpandFile(int iFd, int & iFileSize);	 

  MemTable* GetMemTable(const uint64_t llInstanceID);
  void RemoveMemTable(const uint64_t llInstanceID);
  MemTable* RebuildMemtable(const uint64_t llInstanceID);
	 
private:
  int m_iFd;
  std::string m_sPath;
  ardb::Buffer m_oTmpBuffer;
  ardb::Buffer m_oTmpAppendBuffer;

  std::map<uint64_t, MemTable* > m_memtables;

  std::map<uint64_t, std::string> m_LogFiles;
  int m_resv_filenum;

  std::mutex m_oMutex;
  std::mutex m_oReadMutex;

  int m_iDeletedMaxFileID;
  int m_iMyGroupIdx;

  int m_iNowFileSize;
  int m_iNowFileOffset;
 
private:
  TimeStat m_oTimeStat;
  LogStoreLogger m_oFileLogger;
};
 
}

