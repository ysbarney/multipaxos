/*
 * Module_name: master_mgr.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include <fstream>
#include "master_mgr.h"
#include "comm_include.h"
#include "commdef.h"

namespace multipaxos 
{

MasterMgr :: MasterMgr(const Node * poPaxosNode, const int iGroupIdx, const LogStorage * poLogStorage,
		       const Options & oOptions, MasterChangeCallback pMasterChangeCallback)
  : m_oDefaultMasterSM(poLogStorage, poPaxosNode->GetMyNodeID(), iGroupIdx, pMasterChangeCallback) ,
    m_oMetaDispatch(oOptions, poPaxosNode, iGroupIdx, &m_oDefaultMasterSM){
  m_iLeaseTime = 10000;

  m_poPaxosNode = (Node *)poPaxosNode;
  m_iMyGroupIdx = iGroupIdx;

  m_bIsEnd = false;
  m_bIsStarted = false;

  m_bNeedDropMaster = false;
}

MasterMgr :: ~MasterMgr(){
}

int MasterMgr :: Init(){
  return m_oDefaultMasterSM.Init();
}

void MasterMgr :: SetLeaseTime(const int iLeaseTimeMs){
  if (iLeaseTimeMs < 1000) {
    return;
  }

  m_iLeaseTime = iLeaseTimeMs;
}

void MasterMgr :: DropMaster(){
  m_bNeedDropMaster = true;
}

void MasterMgr :: StopMaster(){
  if (m_bIsStarted){
    m_bIsEnd = true;
    join();
  }
}

void MasterMgr :: RunMaster() {
  start();
  m_oMetaDispatch.start();
}

void MasterMgr :: run(){
  m_bIsStarted = true;

  while(true){
    if (m_bIsEnd){
      return;
    }

    int iLeaseTime = m_iLeaseTime;

    uint64_t llBeginTime = Time::GetSteadyClockMS();

    TryBeMaster(iLeaseTime);

    int iContinueLeaseTimeout = (iLeaseTime - 100) / 4;
    iContinueLeaseTimeout = iContinueLeaseTimeout / 2 + OtherUtils::FastRand() % iContinueLeaseTimeout;

    if (m_bNeedDropMaster){
      m_bNeedDropMaster = false;
      iContinueLeaseTimeout = iLeaseTime * 2;
      PAXOSLOG_INFO << "Need drop master, this round wait time " << iContinueLeaseTimeout << "ms";
    }

    uint64_t llEndTime = Time::GetSteadyClockMS();
    int iRunTime = llEndTime > llBeginTime ? llEndTime - llBeginTime : 0;
    int iNeedSleepTime = iContinueLeaseTimeout > iRunTime ? iContinueLeaseTimeout - iRunTime : 0;

    PAXOSLOG_INFO << "TryBeMaster, sleep time " << iNeedSleepTime << "ms";
    Time::MsSleep(iNeedSleepTime);
  }
}

void MasterMgr :: TryBeMaster(const int iLeaseTime){
  nodeid_t iMasterNodeID = nullnode;
  uint64_t llMasterVersion = 0;

  //step 1 check exist master and get version
  m_oDefaultMasterSM.SafeGetMaster(iMasterNodeID, llMasterVersion);

  if (iMasterNodeID != nullnode && (iMasterNodeID != m_poPaxosNode->GetMyNodeID())){
    PAXOSLOG_INFO << "Other is master, can't try be master, masterid " << iMasterNodeID
		  << " myid " << m_poPaxosNode->GetMyNodeID();

    m_oMetaDispatch.ReportStat(m_poPaxosNode->GetMyNodeID());
    return;
  }

  std::string sMasterStat;
  m_oMetaDispatch.TransactionMaster(m_poPaxosNode->GetMyNodeID(), sMasterStat);

  //step 2 try be master
  std::string sPaxosValue;
  if (!MasterStateMachine::MakeOpValue(m_poPaxosNode->GetMyNodeID(), sMasterStat, llMasterVersion,
				       iLeaseTime, MasterOperatorType_Complete, sPaxosValue)) {
    PAXOSLOG_ERROR << "Make master paxos value fail";
    return;
  }

  const int iMasterLeaseTimeout = iLeaseTime - 100;
  uint64_t llAbsMasterTimeout = Time::GetSteadyClockMS() + iMasterLeaseTimeout;
  uint64_t llCommitInstanceID = 0;

  SMCtx oCtx;
  oCtx.m_iSMID = MASTER_V_SMID;
  oCtx.m_pCtx = (void *)&llAbsMasterTimeout;

  int ret = m_poPaxosNode->MasterPropose(m_iMyGroupIdx, sPaxosValue, llCommitInstanceID, &oCtx);
  if (ret != 0) {
    PAXOSLOG_ERROR << "Master propose failed, ret = " <<  ret ;
  }
}

MasterStateMachine * MasterMgr :: GetMasterSM(){
  return &m_oDefaultMasterSM;
}

MetaDispatch * MasterMgr :: GetMetaDispatch(){
  return &m_oMetaDispatch;
}

}
