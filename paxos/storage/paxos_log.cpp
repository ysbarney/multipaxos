/*
 * Module_name: paxos_log.cpp
 * Author: Barneyliu
 * Time: 2018-12-21
 * Description:
 *
 */

#include "paxos_log.h"
#include "db.h"

namespace multipaxos
{

PaxosLog :: PaxosLog(const LogStorage * poLogStorage)
  : m_poLogStorage((LogStorage *)poLogStorage){
}

PaxosLog :: ~PaxosLog(){
}

int PaxosLog :: WriteLog(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID,
			 const std::string & sValue){
  const int m_iMyGroupIdx = iGroupIdx;

  AcceptorStateData oState;
  oState.set_instanceid(llInstanceID);
  oState.set_acceptedvalue(sValue);
  oState.set_acceptednodeid(nullnode);
  oState.set_checksum(0);

  int ret = WriteState(oWriteOptions, iGroupIdx, llInstanceID, oState);
  if (ret != 0) {
    PAXOSLOG_ERROR << "WriteState to db fail, groupidx " << iGroupIdx << " instanceid " << llInstanceID
		   << " ret " << ret;
    return ret;
  }

  PAXOSLOG_INFO << "OK, groupidx " << iGroupIdx << " InstanceID " << llInstanceID
		<< " valuelen " << sValue.size();
  return 0;
}

int PaxosLog :: ReadLog(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue){
  const int m_iMyGroupIdx = iGroupIdx;

  AcceptorStateData oState;
  int ret = ReadState(iGroupIdx, llInstanceID, oState);
  if (ret != 0) {
    PAXOSLOG_ERROR << "ReadState from db fail, groupidx " << iGroupIdx << " instanceid " << llInstanceID
		   << " ret " << ret;
    return ret;
  }

  sValue = oState.acceptedvalue();

  PAXOSLOG_INFO << "OK, groupidx " << iGroupIdx << " InstanceID " << llInstanceID << " value "
		<< sValue.size();
  return 0;
}

int PaxosLog :: WriteState(const WriteOptions & oWriteOptions, const int iGroupIdx,
			   const uint64_t llInstanceID, const AcceptorStateData & oState){
  const int m_iMyGroupIdx = iGroupIdx;

  string sBuffer;
  bool sSucc = oState.SerializeToString(&sBuffer);
  if (!sSucc) {
    PAXOSLOG_ERROR << "WriteState.Serialize fail";
    return -1;
  }

  int ret = m_poLogStorage->Put(oWriteOptions, iGroupIdx, llInstanceID, sBuffer);
  if (ret != 0) {
    PAXOSLOG_ERROR << "DB.Put fail, groupidx " << iGroupIdx << " bufferlen " << sBuffer.size()
		   << " ret " << ret;
    return ret;
  }

  return 0;
}

int PaxosLog :: ReadState(const int iGroupIdx, const uint64_t llInstanceID, AcceptorStateData & oState){
  const int m_iMyGroupIdx = iGroupIdx;

  string sBuffer;
  int ret = m_poLogStorage->Get(iGroupIdx, llInstanceID, sBuffer);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "DB.Get fail, groupidx " << iGroupIdx << " ret " << ret;
    return ret;
  } else if (ret == 1) {
    PAXOSLOG_INFO << "DB.Get not found, groupidx " << iGroupIdx;
    return 1;
  }

  bool bSucc = oState.ParseFromArray(sBuffer.data(), sBuffer.size());
  if (!bSucc) {
    PAXOSLOG_ERROR << "ReadState.ParseFromArray fail, bufferlen " << sBuffer.size();
    return -1;
  }

  return 0;
}

int PaxosLog :: GetMaxInstanceIDFromLog(const int iGroupIdx, uint64_t & llInstanceID){
  const int m_iMyGroupIdx = iGroupIdx;

  int ret = m_poLogStorage->GetMaxInstanceID(iGroupIdx, llInstanceID);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "DB.GetMax fail, groupidx " << iGroupIdx << " ret " << ret;
  } else if (ret == 1) {
    PAXOSLOG_DEBUG << "MaxInstanceID not exist, groupidx " << iGroupIdx;
  } else {
    PAXOSLOG_INFO << "OK, MaxInstanceID " << llInstanceID << " groupidx " << iGroupIdx;
  } 

  return ret;
}

}

