/*
 * Module_name: meta_dispatch.h
 * Author: Barneyliu
 * Time: 2019-01-16
 * Description:
 *
 */

#pragma once

//#include "base.h"
#include "commdef.h"
#include "comm_include.h"
#include "config_include.h"
#include "node.h"
#include "sm.h"
#include <poll.h>
#include "master_sm.h"

namespace multipaxos 
{

typedef std::pair<nodeid_t, int > stat_pair;
struct SortByNum {
  bool operator() (const stat_pair& lhs, const stat_pair& rhs){
    return lhs.second < rhs.second;
  }
};

class MetaDispatch : public Thread
{
public:
  MetaDispatch(const Options & options, const Node * poPaxosNode,
	       const int iGroupIdx, const MasterStateMachine* DefaultMasterSM);

  virtual ~MetaDispatch();
  void Stop();
  void run();

  int AddMessage(const char * pcMessage, const int iMessageLen);

  //int CheckSlaveStat();
  int ReportStat(const nodeid_t iMyNodeId);
  //tbb::concurrent_hash_map<nodeid_t, int>& GetSlaveStat(nodeid_t iMasterID);
  //std::map<nodeid_t, int>& GetSlaveStat(nodeid_t iMasterID);

  void ParseNodeIDForIP(nodeid_t iMyID, std::string& sIP);
  void UpdateSlaveStat(const char* sBuffer, int iLen);
  void TransactionMaster(const nodeid_t iMasterNodeID ,std::string& MasterStat);
	
public:
  //MasterStateMachine * GetMasterSM();
  void PreAllocateGroupIdx();
	
private:
  int m_iMyGroupIdx;
  Node * m_poPaxosNode;
	
private:
  MasterStateMachine* m_oDefaultMasterSM;
  
  //int m_iLeaseTime;
  int m_iCurStat;  //0 -- master,
                   // 1 -- slave, 2 -- follower

  int m_ihbport;
  int m_iSockFD;
  std::map<nodeid_t, uint64_t> m_mSlaveTime;

  //tbb::concurrent_hash_map<nodeid_t, int> m_mSlaveStat;
  //tbb::concurrent_hash_map<nodeid_t, uint64_t> m_mSlaveTime;

  concurrent_queue<std::string *> m_oMessageQueue;
  int m_iQueueMemSize;

  bool m_bIsEnd;
  bool m_bIsStarted;

  Options m_Options;
};
    
}

