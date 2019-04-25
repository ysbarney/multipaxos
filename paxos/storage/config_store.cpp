/*
 * Module_name: config_store.cpp
 * Author: Barneyliu
 * Time: 2019-04-22
 * Description:
 *
 */

#include "config_store.h"
#include <stdarg.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "crc32.h"
#include "async_boost_log.h"

namespace multipaxos {
  
ConfigStore::ConfigStore() {
  m_iFd = -1;
  b_init = false;
}

ConfigStore::~ConfigStore() {
  if(m_iFd != -1) {
    close(m_iFd);
    m_iFd = -1;
  }
  b_init = false;
}

int ConfigStore::Init(const std::string & sPath, enum ConfigType type, const int iMyGroupIdx) {
  m_iMyGroupIdx = iMyGroupIdx;

  if(type == SystemVarType) {
    m_filename = sPath + "/system_variable";
  } else if(type == MasterVarType) {
    m_filename = sPath + "/master_variable";
  } else if(type == MinChosenType) {
    m_filename = sPath + "/min_chosen";
  } else {
    PAXOSLOG_ERROR << "Config type " << type << " is not supported, please check!";
    return -1;
  }

  m_iFd = open(m_filename.c_str(), O_CREAT | O_RDWR, S_IWRITE | S_IREAD);
  if (m_iFd == -1) {
    PAXOSLOG_ERROR << "open fail, file " << m_filename;
    return -1;
  }

  int iFileLen = lseek(m_iFd, 0, SEEK_END);
  if (iFileLen == -1) {
    PAXOSLOG_ERROR << "lseek fail, file " << m_filename;
    close(m_iFd);
    return -1;
  }
  //PAXOSLOG_INFO << "===iFileLen " << iFileLen;
  if(iFileLen == 0) {
    b_init = true;
    return 0;
  }

  size_t ret = lseek(m_iFd, 0, SEEK_SET);
  if (ret == -1) {
    close(m_iFd);
    m_iFd = -1;
    PAXOSLOG_ERROR << "lseek file handle to 0 failed";
    return -1;
  }

  int iLen = 0;
  ssize_t iReadLen = read(m_iFd, (char *)&iLen, sizeof(int));
  if (iReadLen != (ssize_t)sizeof(int)) {
    close(m_iFd);
    m_iFd = -1;
    PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << sizeof(int);
    return -1;
  }

  if(iLen > 0) {
    if(iLen <= sizeof(uint32_t)) {
      close(m_iFd);
      m_iFd = -1;
      PAXOSLOG_ERROR << "file length " << iLen << " is too small";
      return -1;
    }

    int chkval_len = iLen - sizeof(int);
    if(!m_oTmpBuffer.Reserve(chkval_len)) {
      PAXOSLOG_ERROR << "tmp append buffer reserve len " << chkval_len <<" memory failed";
      close(m_iFd);
      m_iFd = -1;
      return -1;
    }
    
    iReadLen = read(m_iFd, (char*)m_oTmpBuffer.GetRawBuffer(), chkval_len);
    if(iReadLen != chkval_len) {
      close(m_iFd);
      m_iFd = -1;
      PAXOSLOG_ERROR << "readlen " << iReadLen << " not qual to " << chkval_len;
      return -1;
    }

    memcpy(&m_checksum, (char*)m_oTmpBuffer.GetRawBuffer(), sizeof(uint32_t));
    m_value.assign(m_oTmpBuffer.GetRawBuffer() + sizeof(uint32_t), chkval_len - sizeof(uint32_t));
    //PAXOSLOG_INFO << "=== Read iGroupIdx " << m_iMyGroupIdx << " checksum " << m_checksum;
  }

  b_init = true;
  return 0;
}

int ConfigStore::Get(std::string& sValue) {
  if(!b_init) {
    PAXOSLOG_ERROR << "config store is not init, please check";
    return -1;
  }

  sValue = m_value;
  return 0;
}

int ConfigStore::Set(const WriteOptions& oWriteOptions, const std::string& sValue) {
  if(!b_init) {
    PAXOSLOG_ERROR << "config store is not init, please check";
    return -1;
  }

  uint32_t iCheckSum = crc32(0, (const uint8_t *)sValue.c_str(), sValue.size(), CRC32SKIP);
  if(iCheckSum != m_checksum) {
    size_t ret = lseek(m_iFd, 0, SEEK_SET);
    if (ret == -1) {
      close(m_iFd);
      m_iFd = -1;
      PAXOSLOG_ERROR << "lseek file handle to 0 failed";
      return -1;
    }

    int iLen = sizeof(int) + sizeof(uint32_t) + (int)sValue.size();
    if(!m_oTmpBuffer.Reserve(iLen)) {
      PAXOSLOG_ERROR << "tmp append buffer reserve len " << iLen <<" memory failed";
      return -1;
    }

    memcpy((char *)m_oTmpBuffer.GetRawBuffer(), &iLen, sizeof(int));
    memcpy((char *)m_oTmpBuffer.GetRawBuffer() + sizeof(int), &iCheckSum, sizeof(uint32_t));
    memcpy((char *)m_oTmpBuffer.GetRawBuffer() + sizeof(int) + sizeof(uint32_t),
	 sValue.c_str(), sValue.size());

    size_t iWriteLen = write(m_iFd, m_oTmpBuffer.GetRawBuffer(), iLen);

    if (iWriteLen != (size_t)iLen) {
      PAXOSLOG_ERROR << "writelen " << iWriteLen << " not equal to " << iLen
		   << ", buffersize " << sValue.size()	<< " errno " << errno;
      return -1;
    }
    
    m_checksum = iCheckSum;
    m_value = sValue;
    //PAXOSLOG_INFO << "=== Write Fd " << m_iFd << " checksum " << iCheckSum << " iGroupIdx" << m_iMyGroupIdx;
  }

  if (oWriteOptions.bSync) {
    int fdatasync_ret = fdatasync(m_iFd);
    if (fdatasync_ret == -1) {
      PAXOSLOG_ERROR << "fdatasync fail, errno " << errno;
      return -1;
    }
  }
  
  return 0;
}
}
