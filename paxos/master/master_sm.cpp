/*
 * Module_name: master_sm.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "master_sm.h"
#include "master_sm.pb.h"
#include <math.h>
#include <iostream>
#include <sstream>
#include <vector>
#include "commdef.h"
#include "comm_include.h"

namespace multipaxos 
{

MasterStateMachine :: MasterStateMachine(const LogStorage * poLogStorage, const nodeid_t iMyNodeID,
					 const int iGroupIdx, MasterChangeCallback pMasterChangeCallback)
  : m_oMVStore(poLogStorage), m_pMasterChangeCallback(pMasterChangeCallback){
  m_iMyGroupIdx = iGroupIdx;
  m_iMyNodeID = iMyNodeID;

  m_iMasterNodeID = nullnode;
  m_llMasterVersion = (uint64_t)0;
  m_iLeaseTime = 0;
  m_llAbsExpireTime = 0;
}

MasterStateMachine :: ~MasterStateMachine(){
}

////////////////////////////////////////////////////////////////////////////////////////////

int MasterStateMachine :: Init(){
  m_iMasterNodeID = nullnode;
  m_llAbsExpireTime = 0;

  PAXOSLOG_INFO << "OK, master nodeid " << m_iMasterNodeID << " version " << m_llMasterVersion
		<< " expiretime " << m_llAbsExpireTime;

  return 0;
}

int MasterStateMachine :: UpdateMasterToStore(const nodeid_t llMasterNodeID, const uint64_t llVersion,
					      const uint32_t iLeaseTime){
  MasterVariables oVariables;
  oVariables.set_masternodeid(llMasterNodeID);
  oVariables.set_version(llVersion);
  oVariables.set_leasetime(iLeaseTime);

  WriteOptions oWriteOptions;
  oWriteOptions.bSync = true;

  return m_oMVStore.Write(oWriteOptions, m_iMyGroupIdx, oVariables);
}

int MasterStateMachine :: LearnMaster(const uint64_t llInstanceID, const MasterOperator & oMasterOper,
				      const uint64_t llAbsMasterTimeout){
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  PAXOSLOG_DEBUG << "my last version " << m_llMasterVersion << " other last version " <<
    oMasterOper.lastversion() << " this version " << oMasterOper.version() << " instanceid " << llInstanceID;

  if(llInstanceID > m_llMasterVersion
     && oMasterOper.lastversion() != m_llMasterVersion){
    PAXOSLOG_ERROR << "try to fix, set my master version " << m_llMasterVersion << " as other last version "
		   << oMasterOper.lastversion() << ", instanceid " << llInstanceID;
    m_llMasterVersion = llInstanceID;
  }

  bool bMasterChange = false;
  if (m_iMasterNodeID != oMasterOper.nodeid()) {
    bMasterChange = true;
  }

  m_iMasterNodeID = oMasterOper.nodeid();
  if (m_iMasterNodeID == m_iMyNodeID) {
    //self be master
    //use local abstimeout
    m_llAbsExpireTime = llAbsMasterTimeout;

    PAXOSLOG_DEBUG << "Be master success, absexpiretime " << m_llAbsExpireTime;
  } else {
    //other be master
    //use new start timeout
    m_llAbsExpireTime = Time::GetSteadyClockMS() + oMasterOper.timeout();

    PAXOSLOG_DEBUG << "Other be master, absexpiretime " << m_llAbsExpireTime;
  }

  m_iLeaseTime = oMasterOper.timeout();
  m_llMasterVersion = llInstanceID;

  if (bMasterChange) {
    if (m_pMasterChangeCallback != nullptr) {
      m_pMasterChangeCallback(m_iMyGroupIdx, NodeInfo(m_iMasterNodeID), m_llMasterVersion);
    }
  }

  PAXOSLOG_DEBUG << "OK, masternodeid " << m_iMasterNodeID << " version " << m_llMasterVersion
		 << " abstimeout " << m_llAbsExpireTime;
  return 0;
}

void MasterStateMachine :: SafeGetMaster(nodeid_t & iMasterNodeID, uint64_t & llMasterVersion){
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  if (Time::GetSteadyClockMS() >= m_llAbsExpireTime)    {
    PAXOSLOG_DEBUG << "Now " << Time::GetSteadyClockMS() << " AbsExpireTime " << m_llAbsExpireTime;
    iMasterNodeID = nullnode;
  } else {
    iMasterNodeID = m_iMasterNodeID;
  }

  llMasterVersion = m_llMasterVersion;
}

const nodeid_t MasterStateMachine :: GetMaster(const int iGroupIdx){
  nodeid_t iMasterNodeID = nullnode;

  if(m_MasterStatMap.find(iGroupIdx) == m_MasterStatMap.end())
    return nullnode;

  iMasterNodeID = m_MasterStatMap[iGroupIdx];
  return iMasterNodeID;
}


const nodeid_t MasterStateMachine :: GetMasterWithVersion(uint64_t & llVersion) {
  nodeid_t iMasterNodeID = nullnode;
  SafeGetMaster(iMasterNodeID, llVersion);
  return iMasterNodeID;
}

const bool MasterStateMachine :: IsIMMaster(const int iGroupIdx){
  nodeid_t iMasterNodeID = GetMaster(iGroupIdx);
  return iMasterNodeID == m_iMyNodeID;
}

void MasterStateMachine :: UpdateMasterStat(const std::string& sMasterStat){
  PAXOSLOG_INFO << "UpdateMasterStat sMasterStat " << sMasterStat;
  if(sMasterStat.empty())
    return ;

  if(!m_oMVStore.Match_checksum(sMasterStat)) {
    PAXOSLOG_INFO << "sMasterStat is not change, no need update";
    return;
  }

  BuildMasterStatMap(sMasterStat);
  
  WriteOptions oWriteOptions;
  oWriteOptions.bSync = false;
  
  if(m_oMVStore.Write(oWriteOptions, m_iMyGroupIdx, sMasterStat)) {
    PAXOSLOG_ERROR << "sMasterStat write file failed";
  }
}

int MasterStateMachine::BuildMasterStatMap(const std::string& sMasterStat) {
  std::vector<std::string> vmasterStat;
  split_string_vector(sMasterStat, ";", vmasterStat);

  for(size_t i=0; i<vmasterStat.size(); i++) {
    std::string masterStat = vmasterStat[i];
    std::string sgroupId = masterStat.substr(0, masterStat.rfind("@"));
    std::string snodeId = masterStat.substr(masterStat.rfind("@")+1);

    int groupId;
    if(sgroupId == "global")
      groupId = 1000;
    else
      groupId = atoi(sgroupId.c_str());
    
    nodeid_t nodeId = strtoul (snodeId.c_str () , NULL , 10);

    if(m_MasterStatMap.find(groupId) != m_MasterStatMap.end()) {
      if(m_MasterStatMap[groupId] != nodeId)
	m_MasterStatMap[groupId] = nodeId;
    } else
      m_MasterStatMap.insert(std::make_pair(groupId,nodeId));
  }

  return 0;
}

int MasterStateMachine::InitMasterStatMap() {
  std::string sMasterStat;
  if(m_oMVStore.Read(m_iMyGroupIdx, sMasterStat)) {
    PAXOSLOG_ERROR << "get iGroupIdx " << m_iMyGroupIdx << " meta failed";
    return -1;
  }
  if(sMasterStat.empty())
    return 1;
  
  BuildMasterStatMap(sMasterStat);
  return 0;
}

////////////////////////////////////////////////////////////////////////////////////////////

bool MasterStateMachine :: Execute(const int iGroupIdx, const uint64_t llInstanceID,
				   const std::string & sValue, SMCtx * poSMCtx){
  MasterOperator oMasterOper;
  bool bSucc = oMasterOper.ParseFromArray(sValue.data(), sValue.size());
  if (!bSucc) {
    PAXOSLOG_ERROR << "oMasterOper data wrong";
    return false;
  }

  if (oMasterOper.operator_() == MasterOperatorType_Complete){
    uint64_t * pAbsMasterTimeout = nullptr;
    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr) {
      pAbsMasterTimeout = (uint64_t *)poSMCtx->m_pCtx;
    }

    uint64_t llAbsMasterTimeout = pAbsMasterTimeout != nullptr ? *pAbsMasterTimeout : 0;

    PAXOSLOG_DEBUG << "absmaster timeout " << llAbsMasterTimeout;
    int ret = LearnMaster(llInstanceID, oMasterOper, llAbsMasterTimeout);
    if (ret != 0) {
      return false;
    }
    UpdateMasterStat(oMasterOper.masterstat());
  } else {
    PAXOSLOG_ERROR << "unknown op " << oMasterOper.operator_();
    //wrong op, just skip, so return true;
    return true;
  }

  return true;
}

////////////////////////////////////////////////////

bool MasterStateMachine :: MakeOpValue(const nodeid_t iNodeID, const std::string& sMasterStat,
				       const uint64_t llVersion, const int iTimeout,
				       const MasterOperatorType iOp, std::string & sPaxosValue){
  MasterOperator oMasterOper;
  oMasterOper.set_nodeid(iNodeID);
  oMasterOper.set_masterstat(sMasterStat);
  oMasterOper.set_version(llVersion);
  oMasterOper.set_timeout(iTimeout);
  oMasterOper.set_operator_(iOp);
  oMasterOper.set_sid(OtherUtils::FastRand());

  return oMasterOper.SerializeToString(&sPaxosValue);
}

////////////////////////////////////////////////////////////

int MasterStateMachine :: GetCheckpointBuffer(std::string & sCPBuffer){
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  if (m_llMasterVersion == (uint64_t)-1) {
    return 0;
  }

  MasterVariables oVariables;
  oVariables.set_masternodeid(m_iMasterNodeID);
  oVariables.set_version(m_llMasterVersion);
  oVariables.set_leasetime(m_iLeaseTime);

  bool sSucc = oVariables.SerializeToString(&sCPBuffer);
  if (!sSucc) {
    PAXOSLOG_ERROR << "Variables.Serialize fail";
    return -1;
  }

  return 0;
}

int MasterStateMachine :: UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange){
  if (sCPBuffer.size() == 0){
    return 0;
  }

  MasterVariables oVariables;
  bool bSucc = oVariables.ParseFromArray(sCPBuffer.data(), sCPBuffer.size());
  if (!bSucc){
    PAXOSLOG_ERROR << "Variables.ParseFromArray fail, bufferlen " << sCPBuffer.size();
    return -1;
  }

  std::lock_guard<std::mutex> oLockGuard(m_oMutex);

  if (oVariables.version() <= m_llMasterVersion && m_llMasterVersion != (uint64_t)-1) {
    PAXOSLOG_INFO << "log checkpoint, no need update, cp.version " << oVariables.version
		  << " now.version " << m_llMasterVersion;
    return 0;
  }

  int ret = UpdateMasterToStore(oVariables.masternodeid(), oVariables.version(), oVariables.leasetime());
  if (ret != 0) {
    return -1;
  }

  PAXOSLOG_DEBUG << "ok, cp.version " << oVariables.version() << " cp.masternodeid " <<  oVariables.masternodeid()
		 << " old.version " << m_llMasterVersion << " old.masternodeid " << m_iMasterNodeID;
  bool bMasterChange = false;
  m_llMasterVersion = oVariables.version();

  if (oVariables.masternodeid() == m_iMyNodeID) {
    m_iMasterNodeID = nullnode;
    m_llAbsExpireTime = 0;
  } else {
    if (m_iMasterNodeID != oVariables.masternodeid()) {
      bMasterChange = true;
    }
    m_iMasterNodeID = oVariables.masternodeid();
    m_llAbsExpireTime = Time::GetSteadyClockMS() + oVariables.leasetime();
  }

  if (bMasterChange) {
    if (m_pMasterChangeCallback != nullptr) {
      m_pMasterChangeCallback(m_iMyGroupIdx, NodeInfo(m_iMasterNodeID), m_llMasterVersion);
    }
  }

  return 0;
}

////////////////////////////////////////////////////////

void MasterStateMachine :: BeforePropose(const int iGroupIdx, std::string & sValue){
  std::lock_guard<std::mutex> oLockGuard(m_oMutex);
  MasterOperator oMasterOper;
  bool bSucc = oMasterOper.ParseFromArray(sValue.data(), sValue.size());
  if (!bSucc) {
    return;
  }

  oMasterOper.set_lastversion(m_llMasterVersion);
  sValue.clear();
  bSucc = oMasterOper.SerializeToString(&sValue);
  assert(bSucc == true);
} 

const bool MasterStateMachine :: NeedCallBeforePropose(){
  return true;
}

}

