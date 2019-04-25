/*
 * Module_name: master_mgr.h
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#pragma once

#include "commdef.h"
#include "comm_include.h"
#include "node.h"
#include "master_sm.h"
#include "meta_dispatch.h"

namespace multipaxos 
{

class MasterMgr : public Thread
{
public:
  MasterMgr(const Node * poPaxosNode, const int iGroupIdx, const LogStorage * poLogStorage,
	    const Options & oOptions, MasterChangeCallback pMasterChangeCallback);
  virtual ~MasterMgr();

  void RunMaster();
  void StopMaster();
  int Init();
  void run();
  void SetLeaseTime(const int iLeaseTimeMs);
  void TryBeMaster(const int iLeaseTime);
  void DropMaster();

public:
  MasterStateMachine * GetMasterSM();
  MetaDispatch * GetMetaDispatch();
private:
  Node * m_poPaxosNode;
  MasterStateMachine m_oDefaultMasterSM;
  MetaDispatch m_oMetaDispatch;
	
private:
  int m_iLeaseTime;
  bool m_bIsEnd;
  bool m_bIsStarted;
  int m_iMyGroupIdx;
  bool m_bNeedDropMaster;

};
    
}

