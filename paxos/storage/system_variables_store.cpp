/*
 * Module_name: system_variables_store.cpp
 * Author: Barneyliu
 * Time: 2019-01-07
 * Description:
 *
 */

#include "system_variables_store.h"
#include "db.h"

namespace multipaxos
{

SystemVariablesStore :: SystemVariablesStore(const LogStorage * poLogStorage)
  : m_poLogStorage((LogStorage *)poLogStorage){
}

SystemVariablesStore :: ~SystemVariablesStore(){
}

int SystemVariablesStore :: Write(const WriteOptions & oWriteOptions, const int iGroupIdx,
				  const SystemVariables & oVariables){
  const int m_iMyGroupIdx = iGroupIdx;

  string sBuffer;
  bool sSucc = oVariables.SerializeToString(&sBuffer);
  if (!sSucc) {
    PAXOSLOG_ERROR << "Variables.Serialize fail";
    return -1;
  }

  int ret = m_poLogStorage->SetSystemVariables(oWriteOptions, iGroupIdx, sBuffer);
  if (ret != 0) {
    PAXOSLOG_ERROR << "DB.Put fail, groupidx " << iGroupIdx << " bufferlen "
		   << sBuffer.size() << " ret " << ret;
    return ret;
  }

  return 0;
}

int SystemVariablesStore :: Read(const int iGroupIdx, SystemVariables & oVariables){
  const int m_iMyGroupIdx = iGroupIdx;

  string sBuffer;
  int ret = m_poLogStorage->GetSystemVariables(iGroupIdx, sBuffer);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "DB.Get fail, groupidx " << iGroupIdx << " ret " << ret;
    return ret;
  } else if (ret == 1) {
    PAXOSLOG_INFO << "DB.Get not found, groupidx " << iGroupIdx;
    return 1;
  }

  bool bSucc = oVariables.ParseFromArray(sBuffer.data(), sBuffer.size());
  if (!bSucc) {
    PAXOSLOG_ERROR << "Variables.ParseFromArray fail, bufferlen " << sBuffer.size();
    return -1;
  }
  return 0;
}

}

