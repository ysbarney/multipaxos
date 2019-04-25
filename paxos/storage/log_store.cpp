/*
 * Module_name: log_store.cpp
 * Author: Barneyliu
 * Time: 2018-12-19
 * Description:
 *
 */

#include "log_store.h"
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "crc32.h"
#include "async_boost_log.h"

namespace multipaxos
{

MemTable::MemTable(uint64_t logZxid) : m_logZxid(logZxid), m_table(KeyComparator(), &arena_){
}

void MemTable::Add(const char * sbuf, int sbuf_len){
  char* buf = arena_.Allocate(sbuf_len);
  memcpy(buf, (char*)sbuf, sbuf_len);
  m_table.Insert(buf);
}

LogStore :: LogStore(){
  m_iFd = -1;
  m_resv_filenum = 10;

  m_iDeletedMaxFileID = -1;
  m_iMyGroupIdx = -1;
  m_iNowFileSize = -1;
  m_iNowFileOffset = 0;
}

LogStore :: ~LogStore(){
  if (m_iFd != -1) {
    close(m_iFd);
  }
}

int LogStore::UtilGetLogFiles(const std::string& prefix){
  DIR *dirp;
  struct dirent *dp;
  dirp = opendir(m_sPath.c_str());
  if(dirp == NULL) {
    PAXOSLOG_ERROR << "open dir fail, path " << m_sPath;
    return -1;
  }

  while((dp = readdir(dirp)) != NULL) {
    std::string filename;
    filename.assign(dp->d_name);
    std::string filename_prefix = filename.substr(0, filename.rfind("."));
    std::string sIndex = filename.substr(filename.rfind(".")+1);
    if(filename_prefix == prefix) {
      uint64_t fileID = strtoul(sIndex.c_str(),NULL,16);
      m_LogFiles.insert(std::make_pair(fileID, filename));
    }
  }

  closedir(dirp);
  return 0;
}

int LogStore :: Init(const std::string & sPath, const int iMyGroupIdx, int resv_lognum){
  m_iMyGroupIdx = iMyGroupIdx;
  if(access(sPath.c_str(), F_OK) == -1) {
    if(mkdir(sPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
      PAXOSLOG_ERROR << "Create dir fail, path " << sPath << " errno: " << errno;
      return -1;
    }
  }
  
  m_sPath = sPath + "/" + "vfile";
  if (access(m_sPath.c_str(), F_OK) == -1) {
    if (mkdir(m_sPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
      PAXOSLOG_ERROR << "Create dir fail, path " << m_sPath << " errno: " << errno;
      return -1;
    }
  }

  m_resv_filenum = resv_lognum;

  m_oFileLogger.Init(m_sPath);
  int ret = UtilGetLogFiles("log");
  if(ret)
    return ret;

  uint64_t fileID = 0;
  if((int)m_LogFiles.size() == 0) {
    char sFileName[512];
    snprintf(sFileName, sizeof(sFileName), "log.%010x", fileID);
    m_LogFiles.insert(std::make_pair(fileID, sFileName));

    ret = OpenFile(fileID, m_iFd);
    if (ret != 0) {
      return ret;
    }

    ret = RebuildIndex(fileID, m_iNowFileOffset);
    if (!(ret == 0 || ret == -2)) {
      PAXOSLOG_ERROR << "rebuild index fail, ret " << ret;
      return -1;
    }
  } else {
    for(std::map<uint64_t, std::string>::reverse_iterator rit = m_LogFiles.rbegin();
	rit != m_LogFiles.rend(); ) {
      fileID = rit->first;

      ret = OpenFile(fileID, m_iFd);
      if (ret != 0) {
	return ret;
      }

      ret = RebuildIndex(fileID, m_iNowFileOffset);
      if((ret == -2) || fileID) {
	close(m_iFd);
	m_iFd = -1;
	DeleteFile(rit->second);
	m_LogFiles.erase(fileID);
	delete m_memtables[fileID];
	rit++;
	continue;
      }

      if( ret == -1) {
	PAXOSLOG_ERROR << "rebuild index fail, ret " << ret;
	return -1;
      }
      break;
    }
  }

  ret = ExpandFile(m_iFd, m_iNowFileSize);
  if (ret != 0) {
    return ret;
  }

  m_iNowFileOffset = lseek(m_iFd, m_iNowFileOffset, SEEK_SET);
  if (m_iNowFileOffset == -1) {
    PAXOSLOG_ERROR << "seek to now file offset " << m_iNowFileOffset << " fail";
    return -1;
  }

  m_oFileLogger.Log("init write fileid %d now_w_offset %d filesize %d",
		    m_iFd, m_iNowFileOffset, m_iNowFileSize);

  PAXOSLOG_INFO << "ok, path " << m_sPath << " fileid " << m_iFd
		<< " nowfilesize " << m_iNowFileSize << " nowfilewriteoffset " << m_iNowFileOffset;
  return 0;
}

int LogStore :: OpenFile(uint64_t iFileID, int & iFd){
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/log.%010x", m_sPath.c_str(), iFileID);
  iFd = open(sFilePath, O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
  if (iFd == -1) {
    PAXOSLOG_ERROR << "open fail fail, filepath " << sFilePath;
    return -1;
  }

  MemTable *memtable = new MemTable(iFileID);
  m_memtables.insert(std::make_pair(iFileID, memtable));

  PAXOSLOG_INFO << "ok, path " << sFilePath;
  return 0;
}

int LogStore :: GetFileFD(const int iNeedWriteSize, const uint64_t llInstanceID, int & iFd, int & iOffset){
  if (m_iFd == -1) {
    PAXOSLOG_ERROR << "File aready broken, fileid " << m_iFd;
    return -1;
  }

  iOffset = lseek(m_iFd, m_iNowFileOffset, SEEK_SET);
  assert(iOffset != -1);

  if (iOffset + iNeedWriteSize > m_iNowFileSize) {
    close(m_iFd);
    m_iFd = -1;

    int ret = OpenFile(llInstanceID, m_iFd);
    if (ret != 0) {
      m_oFileLogger.Log("new file open file fail, now fileid %d", llInstanceID);
      return ret;
    }

    iOffset = lseek(m_iFd, 0, SEEK_END);
    if (iOffset != 0) {
      assert(iOffset != -1);

      m_oFileLogger.Log("new file but file aready exist, now fileid %d exist filesize %d",
			m_iFd, iOffset);

      PAXOSLOG_ERROR << "IncreaseFileID success, but file exist, data wrong, file size " << iOffset;
      //assert(false);
      return -1;
    }

    ret = ExpandFile(m_iFd, m_iNowFileSize);
    if (ret != 0) {
      PAXOSLOG_ERROR << "new file expand fail, file fd " << m_iFd;

      m_oFileLogger.Log("new file expand file fail, now fileid %d", m_iFd);
      close(m_iFd);
      m_iFd = -1;
      return -1;
    }

    m_oFileLogger.Log("new file expand ok, fileid %d filesize %d", m_iFd, m_iNowFileSize);
  }

  iFd = m_iFd;
  return 0;
}

int LogStore :: ExpandFile(int iFd, int & iFileSize){
  iFileSize = lseek(iFd, 0, SEEK_END);
  if (iFileSize == -1) {
    PAXOSLOG_ERROR << "lseek fail, ret " << iFileSize;
    return -1;
  }

  if (iFileSize == 0){
    //new file
    iFileSize = lseek(iFd, LOG_FILE_MAX_SIZE - 1, SEEK_SET);
    if (iFileSize != LOG_FILE_MAX_SIZE - 1) {
      PAXOSLOG_ERROR << "iFd lseek to dest pos failed."; 
      return -1;
    }

    ssize_t iWriteLen = write(iFd, "\0", 1);
    if (iWriteLen != 1) {
      PAXOSLOG_ERROR << "write 1 bytes fail";
      return -1;
    }

    iFileSize = LOG_FILE_MAX_SIZE;
    int iOffset = lseek(iFd, 0, SEEK_SET);
    m_iNowFileOffset = 0;
    if (iOffset != 0) {
      return -1;
    }
  }

  if(UtilClearLogFile())
    return -1;

  return 0;
}

int LogStore::UtilClearLogFile() {
  std::map<uint64_t, std::string>::iterator it = m_LogFiles.begin();
  int extra_num = (int)m_LogFiles.size() - m_resv_filenum;
  if(extra_num <= 0)
    return 0;

  for(int i=0; i<extra_num; i++) {
    RemoveMemTable(it->first);
    DeleteFile(it->second);
    it++;
  }

  return 0;
}

int LogStore::DeleteFile(const std::string& filename){
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/%s", m_sPath.c_str(), filename.c_str());

  int ret = access(sFilePath, F_OK);
  if (ret == -1) {
    PAXOSLOG_DEBUG << "file already deleted, filepath " << sFilePath;
    ret = 0;
  } else {
    ret = remove(sFilePath);
    if (ret != 0) {
      PAXOSLOG_ERROR << "remove fail, filepath " << sFilePath << " ret " << ret;
    }
  }

  return ret;
}

MemTable* LogStore::GetMemTable(const uint64_t llInstanceID){
  bool existID = false;
  std::map<uint64_t, MemTable*>::reverse_iterator rit = m_memtables.rbegin();
  for(; rit != m_memtables.rend(); rit++) {
    if(rit->first <= llInstanceID) {
      existID = true;
      break;
    }
  }

  if(!existID)
    return NULL;

  return rit->second;
}

void LogStore::RemoveMemTable(const uint64_t llInstanceID){
  bool existID = false;
  std::map<uint64_t, MemTable*>::iterator it = m_memtables.begin();
  for(; it != m_memtables.end(); it++) {
    if(it->first == llInstanceID) {
      existID = true;
      break;
    }
  }

  if(existID) {
    MemTable* memtable = it->second;
    Table_::Iterator iter(&(memtable->get_inntable()));
    while(true) {
      const char* key_ptr;
      if(iter.Valid()) {
	key_ptr = iter.key();
	delete key_ptr;
      } else
	break;

      iter.Next();
    }

    delete memtable;
    m_memtables.erase(it);
  }
  return;	
}

int LogStore :: Append(const WriteOptions & oWriteOptions, const uint64_t llInstanceID,
		       const std::string & sBuffer){
  m_oTimeStat.Point();
  std::lock_guard<std::mutex> oLock(m_oMutex);

  int iFd = -1;
  //int iFileID = -1;
  int iOffset = -1;

  //llInstanceID + sBuffer + iOffset + checksum
  int iLen = sizeof(uint64_t) + sBuffer.size()+sizeof(int)+sizeof(uint32_t);
  
  int iTmpBufferLen = iLen + sizeof(int);

  int ret = GetFileFD(iTmpBufferLen, llInstanceID, iFd, iOffset);
  if (ret != 0) {
    return ret;
  }

  //m_oTmpAppendBuffer.Ready(iTmpBufferLen);
  if(!m_oTmpAppendBuffer.Reserve(iTmpBufferLen)) {
    PAXOSLOG_ERROR << "tmp append buffer reserve len " << iTmpBufferLen <<" memory failed";
    return -1;
  }

  memcpy((char *)m_oTmpAppendBuffer.GetRawBuffer(), &iLen, sizeof(int));
  memcpy((char *)m_oTmpAppendBuffer.GetRawBuffer() + sizeof(int), &llInstanceID, sizeof(uint64_t));
  memcpy((char *)m_oTmpAppendBuffer.GetRawBuffer() + sizeof(int) + sizeof(uint64_t),
	 sBuffer.c_str(), sBuffer.size());
  uint32_t iFileCheckSum = crc32(0, (const uint8_t *)sBuffer.c_str(), sBuffer.size(), CRC32SKIP);

  memcpy((char *)m_oTmpAppendBuffer.GetRawBuffer() + sizeof(int) + sizeof(uint64_t) + sBuffer.size(),
	 &iOffset, sizeof(int));
  memcpy((char *)m_oTmpAppendBuffer.GetRawBuffer() + sizeof(int) + sizeof(uint64_t) + sBuffer.size() + sizeof(int),
	 &iFileCheckSum, sizeof(uint32_t));
  //Insert skiplist
  MemTable *memtable = GetMemTable(llInstanceID);
  if(memtable)
    memtable->Add(m_oTmpAppendBuffer.GetRawBuffer(), iTmpBufferLen);

  size_t iWriteLen = write(iFd, m_oTmpAppendBuffer.GetRawBuffer(), iTmpBufferLen);

  if (iWriteLen != (size_t)iTmpBufferLen) {
    PAXOSLOG_ERROR << "writelen " << iWriteLen << " not equal to " << iTmpBufferLen
		   << ", buffersize " << sBuffer.size()	<< " errno " << errno;
    return -1;
  }

  if (oWriteOptions.bSync) {
    int fdatasync_ret = fdatasync(iFd);
    if (fdatasync_ret == -1) {
      PAXOSLOG_ERROR << "fdatasync fail, writelen " << iWriteLen << " errno " << errno;
      return -1;
    }
  }

  m_iNowFileOffset += iWriteLen;
  int iUseTimeMs = m_oTimeStat.Point();

  return 0;
}

int LogStore :: RebuildIndex(const uint64_t iFileID, int& iNowFileWriteOffset){
  int ret = 0;
  int build_memtable = 0;
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/log.%010x", m_sPath.c_str(), iFileID);

  int iFileLen = lseek(m_iFd, 0, SEEK_END);
  if (iFileLen == -1) {
    close(m_iFd);
    return -1;
  }

  if(iFileLen == 0) {
    iNowFileWriteOffset = 0;
    return -2;
  }

  int writePos = 0;
  while(true) {
    if(writePos >= LOG_FILE_MAX_SIZE)
      break;

    off_t iSeekPos = lseek(m_iFd, writePos, SEEK_SET);
    if (iSeekPos == -1) {
      close(m_iFd);
      return -1;
    }

    int iLen = 0;
    ssize_t iReadLen = read(m_iFd, (char *)&iLen, sizeof(int));
    if (iReadLen != (ssize_t)sizeof(int)) {
      close(m_iFd);
      PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << sizeof(int);
      return -1;
    }

    if(iLen == 0)
      break;

    writePos += (iLen + sizeof(int));
  }

  if(writePos == 0) {
    iNowFileWriteOffset = 0;
    return ret;
  }

  bool bNeedTruncate = false;
  //int filepos = writePos;

  while(true) {
    if(writePos == 0)
      break;

    writePos = writePos - sizeof(int) - sizeof(uint32_t);
    off_t iSeekPos = lseek(m_iFd, writePos, SEEK_SET);
    if (iSeekPos == -1) {
      close(m_iFd);
      return -1;
    }

    int iWriteCurOffset;
    ssize_t iReadLen = read(m_iFd, (char *)&iWriteCurOffset, sizeof(int));
    if (iReadLen != (ssize_t)sizeof(int)) {
      close(m_iFd);
      PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << sizeof(int);
      return -1;
    }

    writePos = iWriteCurOffset;

    iSeekPos = lseek(m_iFd, iWriteCurOffset, SEEK_SET);
    if (iSeekPos == -1) {
      close(m_iFd);
      return -1;
    }

    int iLen = 0;
    iReadLen = read(m_iFd, (char *)&iLen, sizeof(int));
    if (iReadLen != (ssize_t)sizeof(int)) {
      bNeedTruncate = true;
      PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << sizeof(int) << ", need truncate";
      continue;
    }

    if (iLen > LOG_FILE_MAX_SIZE || iLen < (int)sizeof(uint64_t)) {
      PAXOSLOG_ERROR << "File data len wrong, data len " << iLen << " filelen " << LOG_FILE_MAX_SIZE;
      ret = -1;
      break;
    }

    if(!m_oTmpAppendBuffer.Reserve(iLen+sizeof(int))) {
      PAXOSLOG_ERROR << "Apply tmp Append buffer memory len " << iLen+sizeof(int) << " failed";
      ret = -1;
      break;
    }

    iSeekPos = lseek(m_iFd, iWriteCurOffset, SEEK_SET);
    if (iSeekPos == -1) {
      close(m_iFd);
      return -1;
    }

    iReadLen = read(m_iFd, (char *)m_oTmpAppendBuffer.GetRawBuffer(), iLen+sizeof(int));
    if (iReadLen != iLen+sizeof(int)) {
      bNeedTruncate = true;
      PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << iLen+sizeof(int) << ", need truncate";
      continue;
    }

    int msglen = iLen - sizeof(int) - sizeof(uint64_t) - sizeof(uint32_t);
    if(!m_oTmpBuffer.Reserve(msglen)) {
      PAXOSLOG_ERROR << "Apply tmp buffer memory len " << msglen << " failed";
      ret = -1;
      break;
    }

    memcpy((char *)m_oTmpBuffer.GetRawBuffer(),
	   (char *)m_oTmpAppendBuffer.GetRawBuffer() + sizeof(int) + sizeof(uint64_t), msglen);
    uint32_t filechecksum = 0;
    memcpy(&filechecksum,
	   (char *)m_oTmpAppendBuffer.GetRawBuffer() + iLen + sizeof(int) - sizeof(uint32_t), sizeof(uint32_t));

    uint32_t buffChecksum = crc32(0, (const uint8_t *)m_oTmpBuffer.GetRawBuffer(), msglen, CRC32SKIP);
    if(buffChecksum != filechecksum) {
      bNeedTruncate = true;
      PAXOSLOG_ERROR << "readbuf checksum  " << buffChecksum << " not qual to "
		     << filechecksum << ", need truncate";
      continue;
    } else {
      if(build_memtable == 0) 
	iNowFileWriteOffset = iWriteCurOffset+2 * sizeof(int) + sizeof(uint64_t) + sizeof(uint32_t) + msglen;

      build_memtable = 1;
    }

    if(build_memtable) {
      if(bNeedTruncate) {
	PAXOSLOG_ERROR << "log inner checksum failed, so terrible wrong";
	return -1;
      }
    }
    
    MemTable *memtable = GetMemTable(iFileID);
    if(memtable && !bNeedTruncate) {
      memtable->Add(m_oTmpAppendBuffer.GetRawBuffer(), iLen + sizeof(int));
    }
  }

  if(bNeedTruncate) {
    m_oFileLogger.Log("truncate fileid %d offset %d filesize %d",
		      m_iFd, iNowFileWriteOffset, LOG_FILE_MAX_SIZE);
    if (truncate(sFilePath, iNowFileWriteOffset) != 0) {
      PAXOSLOG_ERROR << "truncate fail, file path " << sFilePath << " truncate to length "
		     << iNowFileWriteOffset << " errno " << errno;
      return -1;
    }
  }

  return ret;
}

int LogStore::Read(const uint64_t & llInstanceID, std::string & sBuffer){
  std::lock_guard<std::mutex> oLock(m_oReadMutex);
  int iLen = 0;
  uint64_t seekInstanceID;

  MemTable *memtable = GetMemTable(llInstanceID);
  if(!memtable) {
    memtable = RebuildMemtable(llInstanceID);
  } 

  if(!memtable) {
    return -1;
  }

  Table_::Iterator iter(&(memtable->get_inntable()));
  iter.SeekToFirst();
  while(true) {
    const char* key_ptr;
    if(iter.Valid()) {
      key_ptr = iter.key();
      memcpy(&seekInstanceID, key_ptr+sizeof(int), sizeof(uint64_t));
      if(seekInstanceID == llInstanceID) {
	memcpy(&iLen, key_ptr, sizeof(int));
	sBuffer.assign(key_ptr+sizeof(int) + sizeof(uint64_t),
		       iLen - sizeof(uint64_t) - sizeof(int) - sizeof(uint32_t));
	break;
      }
    } else
      break;

    iter.Next();
  }

  PAXOSLOG_INFO << "ok, instanceid " << llInstanceID << " buffer size " << sBuffer.size();
  return 0;
}

MemTable *LogStore::RebuildMemtable(const uint64_t llInstanceID){
  std::string LogFileName;
  std::map<uint64_t, std::string>::reverse_iterator rit = m_LogFiles.rbegin();
  for(; rit != m_LogFiles.rend(); rit++) {
    if(rit->first < llInstanceID) {
      LogFileName = rit->second;
      break;
    }
  }

  if(!LogFileName.empty()) {
    MemTable *memtable = new MemTable(rit->first);
    m_memtables.insert(std::make_pair(rit->first, memtable));

    int fd = open(LogFileName.c_str(), O_RDONLY);
    if(fd == -1) {
      PAXOSLOG_ERROR << "Log File " << LogFileName << " open failed";
      return NULL;
    }

    int iFileLen = lseek(fd, 0, SEEK_END);
    if (iFileLen == -1) {
      close(fd);
      return NULL;
    }

    if(iFileLen == 0) {
      return NULL;
    }

    int filepos = 0;
    do {
      int iLen = 0;
      int iReadLen = read(fd, (char *)&iLen, sizeof(int));
      if (iReadLen != (ssize_t)sizeof(int)) {
	close(fd);
	PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << sizeof(int);
	return NULL;
      }

      if(iLen == 0)
	break;

      if (iLen > iFileLen || iLen < (int)sizeof(uint64_t)) {
	close(fd);
	PAXOSLOG_ERROR << "File data len wrong, data len " << iLen << " filelen " << iFileLen;
	return NULL;
      }

      off_t iSeekPos = lseek(fd, filepos, SEEK_SET);
      if (iSeekPos == -1){
	close(fd);
	return NULL;
      }

      if(!m_oTmpAppendBuffer.Reserve(iLen+sizeof(int))) {
	close(fd);
	PAXOSLOG_ERROR << "Apply tmp append buffer memory len " << iLen+sizeof(int) << " failed";
	return NULL;
      }

      iReadLen = read(m_iFd, (char *)m_oTmpAppendBuffer.GetRawBuffer(), iLen+sizeof(int));
      if (iReadLen != (iLen+sizeof(int))) {
	close(fd);
	PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << iLen+sizeof(int) ;
	return NULL;
      }

      memtable->Add(m_oTmpAppendBuffer.GetRawBuffer(), iLen+sizeof(int));

      filepos += (iLen + sizeof(int));
    } while(filepos < iFileLen);

    return memtable;
  }

  return NULL;
}

int LogStore :: Del(const uint64_t llInstanceID){
  return 0;
}

int LogStore :: ForceDel(const uint64_t llInstanceID){
  return Del(llInstanceID);
}

int LogStore :: GetMaxInstanceIDFileID(std::string & sFileID, uint64_t & llInstanceID)
{
  std::map<uint64_t, std::string>::reverse_iterator rit = m_LogFiles.rbegin();
  sFileID = rit->second;

  if(m_memtables.size() > 0) {
    std::map<uint64_t, MemTable*>::reverse_iterator rit = m_memtables.rbegin();
    MemTable *memtable = rit->second;
    Table_::Iterator iter(&(memtable->get_inntable()));
    iter.SeekToLast();

    while(true) {
      const char* key_ptr;
      if(iter.Valid()) {
	key_ptr = iter.key();
	memcpy(&llInstanceID, key_ptr+sizeof(int), sizeof(uint64_t));
	break;
      } else
	break;

      iter.Prev();
    }
  }

  return 0;
}

int LogStore :: GetFileIDFromInstanceID(std::string & sFileID, const uint64_t& llInstanceID){
  bool existID = false;
  std::map<uint64_t, std::string>::reverse_iterator rit = m_LogFiles.rbegin();
  for(; rit != m_LogFiles.rend(); rit++) {
    if(rit->first <= llInstanceID) {
      sFileID = rit->second;
      existID = true;
      break;
    }
  }

  if(!existID)
    return -1;

  return 0;
}

/////////////////////////////////////////////////////////////////
LogStoreLogger :: LogStoreLogger()
  : m_iLogFd(-1){
}

LogStoreLogger :: ~LogStoreLogger(){
  if (m_iLogFd != -1) {
    close(m_iLogFd);
  }
}

void LogStoreLogger :: Init(const std::string & sPath){
  char sFilePath[512] = {0};
  snprintf(sFilePath, sizeof(sFilePath), "%s/LOG", sPath.c_str());
  m_iLogFd = open(sFilePath, O_CREAT | O_RDWR | O_APPEND, S_IWRITE | S_IREAD);
}

void LogStoreLogger :: Log(const char * pcFormat, ...){
  if (m_iLogFd == -1) {
    return;
  }

  uint64_t llNowTime = Time::GetTimestampMS();
  time_t tNowTimeSeconds = (time_t)(llNowTime / 1000);
  tm * local_time = localtime(&tNowTimeSeconds);
  char sTimePrefix[64] = {0};
  strftime(sTimePrefix, sizeof(sTimePrefix), "%Y-%m-%d %H:%M:%S", local_time);

  char sPrefix[128] = {0};
  snprintf(sPrefix, sizeof(sPrefix), "%s:%d ", sTimePrefix, (int)(llNowTime % 1000));
  string sNewFormat = string(sPrefix) + pcFormat + "\n";

  char sBuf[1024] = {0};
  va_list args;
  va_start(args, pcFormat);
  vsnprintf(sBuf, sizeof(sBuf), sNewFormat.c_str(), args);
  va_end(args);

  int iLen = strnlen(sBuf, sizeof(sBuf));
  ssize_t iWriteLen = write(m_iLogFd, sBuf, iLen);
  if (iWriteLen != iLen){
    PAXOSLOG_ERROR << "fail, len " << iLen << " writelen " << iWriteLen;
  }
}

}

