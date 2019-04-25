/*
 * Module_name: instance.cpp
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#include "instance.h"
#include "proposer.h"
#include "acceptor.h"
#include "learner.h"

namespace multipaxos
{
Instance :: Instance(const Config * poConfig, const LogStorage * poLogStorage,
		     const MsgTransport * poMsgTransport,const Options & oOptions)
  : m_oSMFac(poConfig->GetMyGroupIdx()),m_oIOLoop((Config *)poConfig, this),
  m_oAcceptor(poConfig, poMsgTransport, this, poLogStorage), 
  m_oLearner(poConfig, poMsgTransport, this, &m_oAcceptor,
	       poLogStorage, &m_oIOLoop, &m_oCheckpointMgr, &m_oSMFac),
  m_oProposer(poConfig, poMsgTransport, this, &m_oLearner, &m_oIOLoop),
  m_oQueueProposer(&m_oProposer),
  m_oPaxosLog(poLogStorage),
  m_oCheckpointMgr((Config *)poConfig, &m_oSMFac, (LogStorage *)poLogStorage,
		     oOptions.bUseCheckpointReplayer),
  m_oOptions(oOptions), m_bStarted(false) {
  m_poConfig = (Config *)poConfig;
  m_poMsgTransport = (MsgTransport *)poMsgTransport;
  m_iCommitTimerID = 0;
  m_iLastChecksum = 0;
}

Instance :: ~Instance() {
  PAXOSLOG_INFO << "Instance Deleted, GroupIdx " << m_poConfig->GetMyGroupIdx();
}

int Instance :: Init() {
  //Must init acceptor first, because the max instanceid is record in acceptor state.
  int ret = m_oAcceptor.Init();
  if (ret != 0) {
    PAXOSLOG_ERROR << "Acceptor.Init fail, ret " << ret;
    return ret;
  }

  ret = m_oCheckpointMgr.Init();
  if (ret != 0) {
    PAXOSLOG_ERROR << "CheckpointMgr.Init fail, ret " << ret;
    return ret;
  }

  //uint64_t llCPInstanceID = m_oCheckpointMgr.GetCheckpointInstanceID() + 1;
  uint64_t llCPInstanceID = m_oCheckpointMgr.GetMinChosenInstanceID() + 1;
  PAXOSLOG_INFO << "Acceptor.OK, Log.InstanceID " << m_oAcceptor.GetInstanceID()
		<< " Checkpoint.InstanceID " << llCPInstanceID;

  uint64_t llNowInstanceID = llCPInstanceID;
  if (llNowInstanceID < m_oAcceptor.GetInstanceID()) {
    ret = PlayLog(llNowInstanceID, m_oAcceptor.GetInstanceID());
    if (ret != 0) {
      return ret;
    }

    PAXOSLOG_INFO << "PlayLog OK, begin instanceid " << llNowInstanceID
		  << " end instanceid " << m_oAcceptor.GetInstanceID();
    llNowInstanceID = m_oAcceptor.GetInstanceID();
  } else {
    if (llNowInstanceID > m_oAcceptor.GetInstanceID()) {
      ret = ProtectionLogic_IsCheckpointInstanceIDCorrect(llNowInstanceID, m_oAcceptor.GetInstanceID());
      if (ret != 0) {
	return ret;
      }
    }
        
    m_oAcceptor.SetInstanceID(llNowInstanceID);
  }

  PAXOSLOG_INFO << "NowInstanceID " << llNowInstanceID;
  m_oLearner.SetInstanceID(llNowInstanceID);
  m_oProposer.SetInstanceID(llNowInstanceID);
  //m_oProposer.SetStartProposalID(m_oAcceptor.GetAcceptorState()->GetPromiseBallot().m_llProposalID + 1);
  m_oCheckpointMgr.SetMaxChosenInstanceID(llNowInstanceID);

  ret = InitLastCheckSum();
  if (ret != 0)  {
    return ret;
  }

  m_oLearner.Reset_AskforLearn_Noop();
  PAXOSLOG_INFO << "OK";
  return 0;
}

void Instance :: Start() {
  //start learner sender
  m_oLearner.StartLearnerSender();
  //start ioloop
  m_oIOLoop.start();
  //start checkpoint replayer and cleaner
  m_oCheckpointMgr.Start();

  m_oQueueProposer.start();
  m_bStarted = true;
}

void Instance :: Stop() {
  if (m_bStarted) {
    m_oIOLoop.Stop();
    m_oCheckpointMgr.Stop();
    m_oLearner.Stop();
  }
}

int Instance :: ProtectionLogic_IsCheckpointInstanceIDCorrect(const uint64_t llCPInstanceID,
							      const uint64_t llLogMaxInstanceID) {
  if (llCPInstanceID <= llLogMaxInstanceID + 1) {
    return 0;
  }

  //checkpoint_instanceid larger than log_maxinstanceid+1 will appear in the following situations 
  //1. Pull checkpoint from other node automatically and restart. (normal case)
  //2. Paxos log was manually all deleted. (may be normal case)
  //3. Paxos log is lost because Options::bSync set as false. (bad case)
  //4. Checkpoint data corruption results an error checkpoint_instanceid. (bad case)
  //5. Checkpoint data copy from other node manually. (bad case)
  //In these bad cases, paxos log between [log_maxinstanceid, checkpoint_instanceid) will not exist
  //and checkpoint data maybe wrong, we can't ensure consistency in this case.

  if (llLogMaxInstanceID == 0) {
    //case 1. Automatically pull checkpoint will delete all paxos log first.
    //case 2. No paxos log. 
    //If minchosen instanceid < checkpoint instanceid.
    //Then Fix minchosen instanceid to avoid that paxos log between
    //[log_maxinstanceid, checkpoint_instanceid) not exist.
    //if minchosen isntanceid > checkpoint.instanceid.
    //That probably because the automatic pull checkpoint did not complete successfully.
    uint64_t llMinChosenInstanceID = m_oCheckpointMgr.GetMinChosenInstanceID();
    if (m_oCheckpointMgr.GetMinChosenInstanceID() != llCPInstanceID) {
      int ret = m_oCheckpointMgr.SetMinChosenInstanceID(llCPInstanceID);
      if (ret != 0) {
	PAXOSLOG_ERROR << "SetMinChosenInstanceID fail, now minchosen "
		       << m_oCheckpointMgr.GetMinChosenInstanceID() << " max instanceid "
		       << llLogMaxInstanceID << " checkpoint instanceid " << llCPInstanceID;
        return -1;
      }

      PAXOSLOG_DEBUG << "Fix minchonse instanceid ok, old minchosen " << llMinChosenInstanceID
		     << " now minchosen " << m_oCheckpointMgr.GetMinChosenInstanceID()
		     << " max " << llLogMaxInstanceID << " checkpoint " << llCPInstanceID;
    }
    return 0;
  } else {
    //other case.
    //PLGErr("checkpoint instanceid %lu larger than log max instanceid %lu. "
    //        "Please ensure that your checkpoint data is correct. "
    //        "If you ensure that, just delete all paxos log data and restart.",
    //        llCPInstanceID, llLogMaxInstanceID);
    return -2;
  }
}

int Instance :: InitLastCheckSum() {
  if (m_oAcceptor.GetInstanceID() == 0) {
    m_iLastChecksum = 0;
    return 0;
  }

  if (m_oAcceptor.GetInstanceID() <= m_oCheckpointMgr.GetMinChosenInstanceID()) {
    m_iLastChecksum = 0;
    return 0;
  }

  AcceptorStateData oState;
  int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), m_oAcceptor.GetInstanceID() - 1, oState);
  if (ret != 0 && ret != 1) {
    return ret;
  }

  if (ret == 1) {
    PAXOSLOG_ERROR << "last checksum not exist, now instanceid " << m_oAcceptor.GetInstanceID();
    m_iLastChecksum = 0;
    return 0;
  }

  m_iLastChecksum = oState.checksum();
  PAXOSLOG_INFO << "ok, last checksum " << m_iLastChecksum;
  return 0;
}

int Instance :: PlayLog(const uint64_t llBeginInstanceID, const uint64_t llEndInstanceID) {
  if (llBeginInstanceID < m_oCheckpointMgr.GetMinChosenInstanceID()) {
    PAXOSLOG_ERROR << "now instanceid " << llBeginInstanceID
		   << " small than min chosen instanceid " << m_oCheckpointMgr.GetMinChosenInstanceID();
    return -2;
  }

  for (uint64_t llInstanceID = llBeginInstanceID; llInstanceID < llEndInstanceID; llInstanceID++) {
    AcceptorStateData oState; 
    int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
    if (ret != 0) {
      PAXOSLOG_ERROR << "log read fail, instanceid " << llInstanceID << " ret " << ret;
      return ret;
    }

    bool bExecuteRet = m_oSMFac.Execute(m_poConfig->GetMyGroupIdx(), llInstanceID,
					oState.acceptedvalue(), nullptr);
    if (!bExecuteRet) {
      PAXOSLOG_ERROR << "Execute fail, instanceid " << llInstanceID;
      return -1;
    }
  }

  return 0;
}

const uint32_t Instance :: GetLastChecksum() {
  return m_iLastChecksum;
}

Cleaner * Instance :: GetCheckpointCleaner() {
  return m_oCheckpointMgr.GetCleaner();
}

Replayer * Instance :: GetCheckpointReplayer(){
  return m_oCheckpointMgr.GetReplayer();
}

QueueProposer* Instance::GetQueueProposer(){
  return &m_oQueueProposer;
}

SMFac* Instance::GetSMFac(){
  return &m_oSMFac;
}


//////////////////////////////////////////////////////////////////////

int Instance :: OnReceiveMessage(const char * pcMessage, const int iMessageLen) {
  m_oIOLoop.AddMessage(pcMessage, iMessageLen);
  return 0;
}

bool Instance :: ReceiveMsgHeaderCheck(const Header & oHeader, const nodeid_t iFromNodeID) {
  if (m_poConfig->GetGid() == 0 || oHeader.gid() == 0) {
    return true;
  }

  if (m_poConfig->GetGid() != oHeader.gid()) {
    PAXOSLOG_ERROR << "Header check fail, header.gid " << oHeader.gid()
		   << " config.gid " << m_poConfig->GetGid() << ", msg.from_nodeid " << iFromNodeID;
    return false;
  }

  return true;
}

void Instance :: OnReceive(const std::string & sBuffer) {
  if (sBuffer.size() <= 6) {
    PAXOSLOG_ERROR << "buffer size " << sBuffer.size() << " too short";
    return;
  }

  Header oHeader;
  size_t iBodyStartPos = 0;
  size_t iBodyLen = 0;
  int ret = Base::UnPackBaseMsg(sBuffer, oHeader, iBodyStartPos, iBodyLen);
  if (ret != 0){
    return;
  }

  int iCmd = oHeader.cmdid();
  if (iCmd == MsgCmd_PaxosMsg) {
    if (m_oCheckpointMgr.InAskforcheckpointMode()) {
      PAXOSLOG_INFO << "in ask for checkpoint mode, ignord paxosmsg";
      return;
    }    

    PaxosMsg oPaxosMsg;
    bool bSucc = oPaxosMsg.ParseFromArray(sBuffer.data() + iBodyStartPos, iBodyLen);
    if (!bSucc) {
      PAXOSLOG_ERROR << "PaxosMsg.ParseFromArray fail, skip this msg";
      return;
    }

    if (!ReceiveMsgHeaderCheck(oHeader, oPaxosMsg.nodeid())){
      return;
    }  
    OnReceivePaxosMsg(oPaxosMsg);
  } else if (iCmd == MsgCmd_CheckpointMsg) {
    CheckpointMsg oCheckpointMsg;
    bool bSucc = oCheckpointMsg.ParseFromArray(sBuffer.data() + iBodyStartPos, iBodyLen);
    if (!bSucc) {
      PAXOSLOG_ERROR << "PaxosMsg.ParseFromArray fail, skip this msg";
      return;
    }

    if (!ReceiveMsgHeaderCheck(oHeader, oCheckpointMsg.nodeid())){
      return;
    }    

    OnReceiveCheckpointMsg(oCheckpointMsg);
  }
}

void Instance :: OnReceiveCheckpointMsg(const CheckpointMsg & oCheckpointMsg) {
  if (oCheckpointMsg.msgtype() == CheckpointMsgType_SendFile) {
    if (!m_oCheckpointMgr.InAskforcheckpointMode()) {
      PAXOSLOG_INFO << "not in ask for checkpoint mode, ignord checkpoint msg";
      return;
    }

    m_oLearner.OnSendCheckpoint(oCheckpointMsg);
  } else if (oCheckpointMsg.msgtype() == CheckpointMsgType_SendFile_Ack) {
    m_oLearner.OnSendCheckpointAck(oCheckpointMsg);
  }
}

int Instance :: OnReceivePaxosMsg(const PaxosMsg & oPaxosMsg, const bool bIsRetry) {
  if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply
            || oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply
            || oPaxosMsg.msgtype() == MsgType_PaxosProposal_SendNewValue) {
    if (!m_poConfig->IsValidNodeID(oPaxosMsg.nodeid())) {
      PAXOSLOG_ERROR << "acceptor reply type msg, from nodeid not in my membership, skip this message";
      return 0;
    }
        
    return ReceiveMsgForProposer(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosPrepare || oPaxosMsg.msgtype() == MsgType_PaxosAccept) {
    if (m_poConfig->GetGid() == 0) {
      m_poConfig->AddTmpNodeOnlyForLearn(oPaxosMsg.nodeid());
    }
        
    if ((!m_poConfig->IsValidNodeID(oPaxosMsg.nodeid()))) {
      PAXOSLOG_ERROR << "prepare/accept type msg, from nodeid not in my membership(or i'm null membership)" 
                    ", skip this message and add node to tempnode, my gid " << m_poConfig->GetGid();
      m_poConfig->AddTmpNodeOnlyForLearn(oPaxosMsg.nodeid());
      return 0;
    }

    ChecksumLogic(oPaxosMsg);
    return ReceiveMsgForAcceptor(oPaxosMsg, bIsRetry);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforLearn
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_ProposerSendSuccess
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_ComfirmAskforLearn
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendNowInstanceID
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue_Ack
            || oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforCheckpoint) {
    ChecksumLogic(oPaxosMsg);
    return ReceiveMsgForLearner(oPaxosMsg);
  } else {
    PAXOSLOG_ERROR << "Invaid msgtype " << oPaxosMsg.msgtype();
  }
  return 0;
}

int Instance :: ReceiveMsgForProposer(const PaxosMsg & oPaxosMsg) {
  if (m_poConfig->IsIMFollower()) {
    PAXOSLOG_ERROR << "I'm follower, skip this message";
    return 0;
  }

///////////////////////////////////////////////////////////////
#if 0
  if (oPaxosMsg.instanceid() != m_oProposer.GetInstanceID()) {
    if (oPaxosMsg.instanceid() + 1 == m_oProposer.GetInstanceID()) {
      //Exipred reply msg on last instance.
      //If the response of a node is always slower than the majority node, 
      //then the message of the node is always ignored even if it is a reject reply.
      //In this case, if we do not deal with these reject reply, the node that 
      //gave reject reply will always give reject reply. 
      //This causes the node to remain in catch-up state.
      //
      //To avoid this problem, we need to deal with the expired reply.
      if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply) {
	m_oProposer.OnExpiredPrepareReply(oPaxosMsg);
      } else if (oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply) {
	m_oProposer.OnExpiredAcceptReply(oPaxosMsg);
      }
    }

    return 0;
  }
#endif

  if (oPaxosMsg.msgtype() == MsgType_PaxosPrepareReply){
    m_oProposer.OnPrepareReply(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosAcceptReply) {
    m_oProposer.OnAcceptReply(oPaxosMsg);
  }

  return 0;
}

int Instance :: ReceiveMsgForAcceptor(const PaxosMsg & oPaxosMsg, const bool bIsRetry){
  if (m_poConfig->IsIMFollower())    {
    PAXOSLOG_ERROR << "I'm follower, skip this message";
    return 0;
  }

  //////////////////////////////////////////////////////////////
#if 0
  if (oPaxosMsg.instanceid() != m_oAcceptor.GetInstanceID()) {
    ;
  }

  if (oPaxosMsg.instanceid() == m_oAcceptor.GetInstanceID() + 1) {
    //skip success message
    PaxosMsg oNewPaxosMsg = oPaxosMsg;
    oNewPaxosMsg.set_instanceid(m_oAcceptor.GetInstanceID());
    oNewPaxosMsg.set_msgtype(MsgType_PaxosLearner_ProposerSendSuccess);

    ReceiveMsgForLearner(oNewPaxosMsg);
  }
#endif

  if (oPaxosMsg.instanceid() >= m_oAcceptor.GetInstanceID()) {
    if (oPaxosMsg.msgtype() == MsgType_PaxosPrepare) {
      return m_oAcceptor.OnPrepare(oPaxosMsg);
    } else if (oPaxosMsg.msgtype() == MsgType_PaxosAccept) {
      m_oAcceptor.OnAccept(oPaxosMsg);
    }
  } else if ((!bIsRetry) && (oPaxosMsg.instanceid() > m_oAcceptor.GetInstanceID())) {
    //retry msg can't retry again.
    if (oPaxosMsg.instanceid() >= m_oLearner.GetSeenLatestInstanceID()) {
      if (oPaxosMsg.instanceid() < m_oAcceptor.GetInstanceID() + RETRY_QUEUE_MAX_LEN) {
	//need retry msg precondition
	//1. prepare or accept msg
	//2. msg.instanceid > nowinstanceid.
	//    (if < nowinstanceid, this msg is expire)
	//3. msg.instanceid >= seen latestinstanceid.
	//    (if < seen latestinstanceid, proposer don't need reply with this instanceid anymore.)
	//4. msg.instanceid close to nowinstanceid.
	m_oIOLoop.AddRetryPaxosMsg(oPaxosMsg);
      } else {
	//retry msg not series, no use.
	m_oIOLoop.ClearRetryQueue();
      }
    }
  }

  return 0;
}

int Instance :: ReceiveMsgForLearner(const PaxosMsg & oPaxosMsg){
  if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforLearn) {
    m_oLearner.OnAskforLearn(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue) {
    m_oLearner.OnSendLearnValue(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_ProposerSendSuccess) {
    m_oLearner.OnProposerSendSuccess(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendNowInstanceID) {
    m_oLearner.OnSendNowInstanceID(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_ComfirmAskforLearn) {
    m_oLearner.OnComfirmAskForLearn(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_SendLearnValue_Ack) {
    m_oLearner.OnSendLearnValue_Ack(oPaxosMsg);
  } else if (oPaxosMsg.msgtype() == MsgType_PaxosLearner_AskforCheckpoint) {
    m_oLearner.OnAskforCheckpoint(oPaxosMsg);
  }

  if (m_oLearner.IsLearned()) {
    SMCtx * poSMCtx = nullptr;

    if (!SMExecute(m_oLearner.GetInstanceID(), m_oLearner.GetLearnValue(), poSMCtx)) {
      PAXOSLOG_ERROR << "SMExecute fail, instanceid " << m_oLearner.GetInstanceID()
		     << ", not increase instanceid";

      m_oProposer.CancelSkipPrepare();
      return -1;
    }

    {
      //this paxos instance end, tell proposal done
      //m_oCommitCtx.SetResult(PaxosTryCommitRet_OK
      //        , m_oLearner.GetInstanceID(), m_oLearner.GetLearnValue());

      if (m_iCommitTimerID > 0) {
	m_oIOLoop.RemoveTimer(m_iCommitTimerID);
      }
    }

    m_oLearner.InitForNewPaxosInstance();

    //PLGHead("[Learned] New paxos instance has started, Now.Proposer.InstanceID %lu "
    //        "Now.Acceptor.InstanceID %lu Now.Learner.InstanceID %lu",
    //        m_oProposer.GetInstanceID(), m_oAcceptor.GetInstanceID(), m_oLearner.GetInstanceID());

    m_oCheckpointMgr.SetMaxChosenInstanceID(m_oAcceptor.GetInstanceID());
  }

  return 0;
}

const uint64_t Instance :: GetNowInstanceID() {
  return m_oAcceptor.GetInstanceID();
}

const uint64_t Instance :: GetMinChosenInstanceID(){
  return m_oCheckpointMgr.GetMinChosenInstanceID();
}

///////////////////////////////

void Instance :: OnTimeout(const uint32_t iTimerID, const int iType, const uint64_t llInstanceID){
  if (iType == Timer_Proposer_Prepare_Timeout) {
    m_oProposer.OnPrepareTimeout(llInstanceID);
  } else if (iType == Timer_Proposer_Accept_Timeout) {
    m_oProposer.OnAcceptTimeout(llInstanceID);
  } else if (iType == Timer_Learner_Askforlearn_noop) {
    m_oLearner.AskforLearn_Noop();
  } else {
    PAXOSLOG_ERROR << "unknown timer type " << iType << ", timerid " << iTimerID;
  }
}

////////////////////////////////

void Instance :: AddStateMachine(StateMachine * poSM){
  m_oSMFac.AddSM(poSM);
}

bool Instance :: SMExecute(const uint64_t llInstanceID, const std::string & sValue,
			   SMCtx * poSMCtx){
  return m_oSMFac.Execute(m_poConfig->GetMyGroupIdx(), llInstanceID, sValue, poSMCtx);
}

////////////////////////////////

void Instance :: ChecksumLogic(const PaxosMsg & oPaxosMsg){
  if (oPaxosMsg.lastchecksum() == 0) {
    return;
  }

  if (oPaxosMsg.instanceid() != m_oAcceptor.GetInstanceID()) {
    return;
  }

  if (m_oAcceptor.GetInstanceID() > 0 && GetLastChecksum() == 0) {
    PAXOSLOG_ERROR << "I have no last checksum, other last checksum " << oPaxosMsg.lastchecksum();
    m_iLastChecksum = oPaxosMsg.lastchecksum();
    return;
  }

  PAXOSLOG_INFO << "my last checksum " << GetLastChecksum() << " other last checksum "
		<< oPaxosMsg.lastchecksum();

  if (oPaxosMsg.lastchecksum() != GetLastChecksum()) {
    PAXOSLOG_ERROR << "checksum fail, my last checksum " << GetLastChecksum()
		   << " other last checksum " << oPaxosMsg.lastchecksum();
  }

  assert(oPaxosMsg.lastchecksum() == GetLastChecksum());
}

//////////////////////////////////////////

int Instance :: GetInstanceValue(const uint64_t llInstanceID, std::string & sValue, int & iSMID){
  iSMID = 0;

  if (llInstanceID >= m_oAcceptor.GetInstanceID()){
    return Paxos_GetInstanceValue_Value_Not_Chosen_Yet;
  }

  AcceptorStateData oState;
  int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
  if (ret != 0 && ret != 1){
    return -1;
  }

  if (ret == 1) {
    return Paxos_GetInstanceValue_Value_NotExist;
  }

  memcpy(&iSMID, oState.acceptedvalue().data(), sizeof(int));
  sValue = string(oState.acceptedvalue().data() + sizeof(int),
		  oState.acceptedvalue().size() - sizeof(int));

  return 0;
}

}
