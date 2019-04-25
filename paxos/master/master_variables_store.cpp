/*
 * Module_name: master_variables_store.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "master_variables_store.h"
#include "db.h"

namespace multipaxos
{

MasterVariablesStore :: MasterVariablesStore(const LogStorage * poLogStorage)
  : m_poLogStorage((LogStorage *)poLogStorage){
  m_ichksum = 0;
}

MasterVariablesStore :: ~MasterVariablesStore(){
}

int MasterVariablesStore :: Write(const WriteOptions & oWriteOptions, const int iGroupIdx,
				  const MasterVariables & oVariables) {
  string sBuffer;
  bool sSucc = oVariables.SerializeToString(&sBuffer);
  if (!sSucc) {
    PAXOSLOG_ERROR << "Variables.Serialize fail";
    return -1;
  }

  int ret = m_poLogStorage->SetMasterVariables(oWriteOptions, iGroupIdx, sBuffer);
  if (ret != 0) {
    PAXOSLOG_ERROR << "DB.Put fail, groupid " << iGroupIdx << " bufferlen " << sBuffer.size() << " ret " << ret;
    return ret;
  }

  return 0;
}

int MasterVariablesStore::Write(const WriteOptions& oWriteOptions, const int iGroupIdx, const std::string& sValue) {
  int ret = m_poLogStorage->SetMasterVariables(oWriteOptions, iGroupIdx, sValue);
  if(ret != 0) {
    PAXOSLOG_ERROR << "DB.Put fail, groupid " << iGroupIdx << " bufferlen " << sValue.size() << " ret " << ret;
    return ret;
  }
  
  return 0;
}

int MasterVariablesStore :: Read(const int iGroupIdx, MasterVariables & oVariables){
  string sBuffer;
  int ret = m_poLogStorage->GetMasterVariables(iGroupIdx, sBuffer);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "DB.Get faile, groupid " << iGroupIdx << " ret " << ret;
    return ret;
  } else if (ret == 1) {
    PAXOSLOG_INFO << "DB.Get not found, groupidx " << iGroupIdx;
    return 1;
  }

  bool bSucc = oVariables.ParseFromArray(sBuffer.data(), sBuffer.size());
  if (!bSucc) {
    PAXOSLOG_ERROR << "Variables.ParseFromArray fail, bufferlen " <<  sBuffer.size();
    return -1;
  }

  return 0;
}

int MasterVariablesStore::Read(const int iGroupIdx, std::string& sValue) {
  string sBuffer;
  int ret = m_poLogStorage->GetMasterVariables(iGroupIdx, sBuffer);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "DB.Get faile, groupid " << iGroupIdx << " ret " << ret;
    return ret;
  } else if (ret == 1) {
    PAXOSLOG_INFO << "DB.Get not found, groupidx " << iGroupIdx;
    return 1;
  }

  return 0;
}

bool MasterVariablesStore::Match_checksum(const std::string& sValue) {
  uint32_t ichksum = crc32(0, (const uint8_t *)sValue.c_str(), sValue.size(), CRC32SKIP);
  if(m_ichksum != ichksum) {
    m_ichksum = ichksum;
    return true;
  }
  
  return false;
}

}

