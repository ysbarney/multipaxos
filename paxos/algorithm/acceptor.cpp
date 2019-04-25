/*
 * Module_name: acceptor.cpp
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */
 
#include "acceptor.h"

namespace multipaxos 
{
Acceptor :: Acceptor(const Config * poConfig, const MsgTransport * poMsgTransport,
		     const Instance * poInstance,const LogStorage * poLogStorage)
  : Base(poConfig, poMsgTransport, poInstance), m_oPaxosLog(poLogStorage) {
  m_poConfig = (Config *)poConfig;
  m_llInstanceID = 0;
}

Acceptor :: ~Acceptor(){
}

int Acceptor::Init() {
  m_iSyncTimes = 0;
  int ret = Load(m_llInstanceID);
  if(ret) {
    PAXOSLOG_ERROR << "Acceptor init load failed";
    return ret;
  }
  
  SetInstanceID(m_llInstanceID);
  return 0;
}

void Acceptor::InitForNewPaxosInstance(){

}

int Acceptor :: OnPrepare(const PaxosMsg & oPaxosMsg){
  PAXOSLOG_DEBUG << "OnPrepare START Msg.InstanceID " << oPaxosMsg.instanceid() 
		<< " Msg.from_nodeid " << oPaxosMsg.nodeid();

  PaxosMsg oReplyPaxosMsg;
  oReplyPaxosMsg.set_instanceid(oPaxosMsg.instanceid());
  oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  //oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
  oReplyPaxosMsg.set_msgtype(MsgType_PaxosPrepareReply);

  if(oPaxosMsg.instanceid() >= m_llInstanceID) {
    m_llInstanceID = oPaxosMsg.instanceid();
    oReplyPaxosMsg.set_rejectbypromiseid(0);
  } else {
    oReplyPaxosMsg.set_rejectbypromiseid(m_llInstanceID);
  }

  nodeid_t iReplyNodeID = oPaxosMsg.nodeid();
  SendMessage(iReplyNodeID, oReplyPaxosMsg);
  return 0;
}

void Acceptor :: OnAccept(const PaxosMsg & oPaxosMsg) {
  PAXOSLOG_DEBUG << "START Msg.InstanceID " << oPaxosMsg.instanceid() << " Msg.from_nodeid "
		 <<oPaxosMsg.nodeid() << " Msg.ValueLen " << oPaxosMsg.value().size();

  PaxosMsg oReplyPaxosMsg;
  oReplyPaxosMsg.set_instanceid(oPaxosMsg.instanceid());
  oReplyPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
  //oReplyPaxosMsg.set_proposalid(oPaxosMsg.proposalid());
  oReplyPaxosMsg.set_msgtype(MsgType_PaxosAcceptReply);

  if(oPaxosMsg.instanceid() >= m_llInstanceID) {
    m_llInstanceID = oPaxosMsg.instanceid();
    oReplyPaxosMsg.set_rejectbypromiseid(0);
    if(persist(oPaxosMsg)) {
      PAXOSLOG_ERROR << "persist paxos log failed";
      return;
    }
  } else {
    PAXOSLOG_DEBUG << "[Reject] State.PromiseID " << m_llInstanceID;
    oReplyPaxosMsg.set_rejectbypromiseid(m_llInstanceID);
  }
  SendMessage(oPaxosMsg.nodeid(), oReplyPaxosMsg);
}

int Acceptor :: persist(const PaxosMsg & oPaxosMsg) {
  AcceptorStateData oState;
  oState.set_instanceid(oPaxosMsg.instanceid());
  oState.set_acceptednodeid(oPaxosMsg.nodeid());
  oState.set_acceptedvalue(oPaxosMsg.value());
  uint32_t iCheckSum = crc32(0, (const uint8_t*)oPaxosMsg.value().c_str(),
			     oPaxosMsg.value().size(), CRC32SKIP);
  oState.set_checksum(iCheckSum);

  //m_oAcceptorWrite.AddMessage(sBuffer.c_str(), (int)sBuffer.size());
  WriteOptions oWriteOptions;
  oWriteOptions.bSync = m_poConfig->LogSync();
  if (oWriteOptions.bSync) {
    m_iSyncTimes++;
    if (m_iSyncTimes > m_poConfig->SyncInterval()) {
      m_iSyncTimes = 0;
    } else {
      oWriteOptions.bSync = false;
    }
  }

  int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(),
				   oPaxosMsg.instanceid(), oState);
  if (ret != 0) {
    return ret;
  }
	
  return 0;
}

int Acceptor::Load(uint64_t & llInstanceID) {
  int ret = m_oPaxosLog.GetMaxInstanceIDFromLog(m_poConfig->GetMyGroupIdx(), llInstanceID);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "Load max instance id fail, ret " << ret;
    return ret;
  }

  if (ret == 1) {
    PAXOSLOG_ERROR << "empty database";
    llInstanceID = 0;
    return 0;
  }
  
  return 0;
}

}
