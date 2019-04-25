/*
 * Module_name: meta_dispatch.cpp
 * Author: Barneyliu
 * Time: 2019-01-16
 * Description:
 *
 */

#include "meta_dispatch.h"
 
namespace multipaxos 
{
 
#define Delta_MAX 5000

MetaDispatch :: MetaDispatch(const Options & options, const Node * poPaxosNode,
			     const int iGroupIdx, const MasterStateMachine* DefaultMasterSM)
  :  m_Options(options){
  m_iMyGroupIdx = iGroupIdx;
  m_poPaxosNode = (Node *)poPaxosNode;
  m_oDefaultMasterSM = (MasterStateMachine *)DefaultMasterSM;
  
  PreAllocateGroupIdx();
  m_bIsEnd = false;
  m_bIsStarted = false;
  m_iQueueMemSize = 0;
}

MetaDispatch :: ~MetaDispatch(){
}

void MetaDispatch::Stop(){
  if (m_bIsStarted){
    m_bIsStarted = false;
    m_bIsEnd = true;
    join();
  }
}

void MetaDispatch :: run(){
  m_bIsStarted = true;

  while(true){
    if (m_bIsEnd){
      return;
    }

    std::string * psMessage = nullptr;

    bool bSucc = m_oMessageQueue.peek(psMessage, 500);
    if(bSucc) {
      if (psMessage != nullptr && psMessage->size() > 0){
	m_iQueueMemSize -= psMessage->size();
	//m_poInstance->OnReceive(*psMessage);
	UpdateSlaveStat((*psMessage).c_str(), psMessage->size());
      }

      delete psMessage;
    }
  }
}

void MetaDispatch :: UpdateSlaveStat(const char* sBuffer, int iLen){
  PAXOSLOG_INFO << "Master update meta " << sBuffer;
  std::string rbuff;
  rbuff.assign(sBuffer, iLen);
  std::string sMyNodeID = rbuff.substr(rbuff.rfind("@")+1);

  nodeid_t MyNodeID = strtoul (sMyNodeID.c_str () , NULL , 10);

  if(m_mSlaveTime.find(MyNodeID) != m_mSlaveTime.end()) {
    m_mSlaveTime[MyNodeID] = Time::GetSteadyClockMS();
  } else
    m_mSlaveTime.insert(std::make_pair(MyNodeID, Time::GetSteadyClockMS()));
}

int MetaDispatch :: ReportStat(const nodeid_t iMyNodeId){
  int ret = 0;
  uint64_t llCommitInstanceID = 0;
  std::string hbstat = "ok@"+std::to_string(iMyNodeId);
  ret = m_poPaxosNode->ReportStatus(0, hbstat, llCommitInstanceID, NULL);
  return ret;
}

void MetaDispatch::ParseNodeIDForIP(nodeid_t iMyID, std::string& sIP){
  in_addr addr;
  addr.s_addr = iMyID >> 32;

  sIP = std::string(inet_ntoa(addr));
}

int MetaDispatch::AddMessage(const char * pcMessage, const int iMessageLen)
{
  while(true) {
    if ((int)m_oMessageQueue.size() > QUEUE_MAXLENGTH){
      PAXOSLOG_ERROR << "Queue full, skip msg";
      usleep(1000 * 20);
      continue;
    }

    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE){
      PAXOSLOG_ERROR << "queue memsize " << m_iQueueMemSize << " too large, can't enqueue";
      usleep(1000 * 20);
      continue;
    }
    break;
  }

  m_oMessageQueue.add(new string(pcMessage, iMessageLen));
  m_iQueueMemSize += iMessageLen;
  return 0;
}

void MetaDispatch::TransactionMaster(const nodeid_t iMasterNodeID ,std::string& MasterStat){
  /*uint64_t curTime = Time::GetSteadyClockMS();
	for(std::map<nodeid_t, uint64_t>::iterator it=m_mSlaveTime.begin(); it!=m_mSlaveTime.end(); it++) {
		uint64_t reportTime = it->second;
		nodeid_t slaveid = it->first;

		if(slaveid != iMasterNodeID) {
		 	if(m_mSlaveStat.find(slaveid) != m_mSlaveStat.end()) {
				if(curTime - reportTime > Delta_MAX)
					m_mSlaveStat[slaveid] = 0;
				else 
					m_mSlaveStat[slaveid] = 1;
			} else
				m_mSlaveStat.insert(std::make_pair(slaveid, 1));
		 }
	}*/

  std::map<int, nodeid_t> oMasterStat = m_oDefaultMasterSM->GetMasterStat();
  
  if(oMasterStat[0] != iMasterNodeID) {
    nodeid_t tmpnodeID = oMasterStat[0];
    for(std::map<int, nodeid_t>::iterator it=oMasterStat.begin(); it!=oMasterStat.end(); it++) {
      if(it->second == iMasterNodeID) {
	oMasterStat[it->first] = tmpnodeID;
	break;
      }
    }

    oMasterStat[0] = iMasterNodeID;
  }

  MasterStat = MasterStat + "global@"+std::to_string(iMasterNodeID)+";";
  for(std::map<int, nodeid_t>::iterator it=oMasterStat.begin(); it != oMasterStat.end(); it++) {
    if(it->first == 1000)
      continue;
    
    MasterStat = MasterStat + std::to_string(it->first)+"@"+std::to_string(it->second)+";";
  }

  return;
}

void MetaDispatch::PreAllocateGroupIdx() {
  int ret = m_oDefaultMasterSM->InitMasterStatMap();
  if(ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "Master sm init master stat map failed";
    return;
  }
  
  if(ret == 1) {
    std::map<int, nodeid_t> oMasterStat = m_oDefaultMasterSM->GetMasterStat();
    
    std::map<nodeid_t, int> m_nodeMapNum;
    for(size_t i=0; i<m_Options.vecNodeInfoList.size(); i++) {
      nodeid_t nodeId = m_Options.vecNodeInfoList[i].GetNodeID();
      m_nodeMapNum.insert(std::make_pair(nodeId, 0));
    }

    for(int i=0; i<m_Options.iGroupCount; i++) {
      std::vector<stat_pair> stat_pair_vec(m_nodeMapNum.begin(), m_nodeMapNum.end());
      std::sort(stat_pair_vec.begin(), stat_pair_vec.end(), SortByNum());

      m_nodeMapNum[stat_pair_vec[0].first] += 1;
      oMasterStat.insert(std::make_pair(i, stat_pair_vec[0].first));
    }

    m_oDefaultMasterSM->SetMasterStat(oMasterStat);
  } 
}

}

