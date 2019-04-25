/*
 * Module_name: acceptor.h
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#pragma once
 
#include "base.h"
#include <string>
#include "comm_include.h"
#include "paxos_log.h"

namespace multipaxos
{

class Acceptor : public Base, public gensequence
{
public:
  Acceptor(const Config * poConfig, const MsgTransport * poMsgTransport,
	   const Instance * poInstance,const LogStorage * poLogStorage);
  virtual ~Acceptor();
  virtual void InitForNewPaxosInstance();
  int Init();
  int OnPrepare(const PaxosMsg & oPaxosMsg);
  void OnAccept(const PaxosMsg & oPaxosMsg);
  int persist(const PaxosMsg & oPaxosMsg);
  int Load(uint64_t & llInstanceID);

  uint64_t GetInstanceID() {
    return m_llInstanceID;
  }
		
private:
  uint64_t m_llInstanceID;
  Config * m_poConfig;
  PaxosLog m_oPaxosLog;
  int m_iSyncTimes;
};
	 
}

