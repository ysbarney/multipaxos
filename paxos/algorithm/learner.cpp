/*
 * Module_name: learner.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "learner.h"
#include "acceptor.h"
#include "crc32.h"
#include "cp_mgr.h"
#include "sm_base.h"

namespace multipaxos
{

LearnerState :: LearnerState(const Config * poConfig, const LogStorage * poLogStorage)
  : m_oPaxosLog(poLogStorage){
  m_poConfig = (Config *)poConfig;

  Init();
}

LearnerState :: ~LearnerState(){
}

void LearnerState :: Init(){
  m_sLearnedValue = "";
  m_bIsLearned = false;
  m_iNewChecksum = 0;
}

const uint32_t LearnerState :: GetNewChecksum() const{
  return m_iNewChecksum;
}

void LearnerState :: LearnValueWithoutWrite(const uint64_t llInstanceID,
					    const std::string & sValue, const uint32_t iNewChecksum){
  m_sLearnedValue = sValue;
  m_bIsLearned = true;
  m_iNewChecksum = iNewChecksum;
}

int LearnerState :: LearnValue(const uint64_t llInstanceID, const nodeid_t& llNodeID,
			       const std::string & sValue, const uint32_t iLastChecksum){
  if (llInstanceID > 0 && iLastChecksum == 0){
    m_iNewChecksum = 0;
  } else if (sValue.size() > 0) {
    m_iNewChecksum = crc32(iLastChecksum, (const uint8_t *)sValue.data(), sValue.size(), CRC32SKIP);
  }

  AcceptorStateData oState;
  oState.set_instanceid(llInstanceID);
  oState.set_acceptedvalue(sValue);
  oState.set_acceptednodeid(llNodeID);
  oState.set_checksum(m_iNewChecksum);

  WriteOptions oWriteOptions;
  oWriteOptions.bSync = false;

  int ret = m_oPaxosLog.WriteState(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID, oState);
  if (ret != 0) {
    PAXOSLOG_ERROR << "LogStorage.WriteLog fail, InstanceID " << llInstanceID << " ValueLen "
		   << sValue << " ret " << ret;
    return ret;
  }

  LearnValueWithoutWrite(llInstanceID, sValue, m_iNewChecksum);

  PAXOSLOG_DEBUG << "OK, InstanceID " << llInstanceID << " ValueLen "
		 << sValue << " checksum " << m_iNewChecksum;

  return 0;
}

const std::string & LearnerState :: GetLearnValue(){
  return m_sLearnedValue;
}

const bool LearnerState :: GetIsLearned(){
  return m_bIsLearned;
}

//////////////////////////////////////////////////////////////////////////////

Learner :: Learner(const Config * poConfig, const MsgTransport * poMsgTransport,
		   const Instance * poInstance, const Acceptor * poAcceptor,
		   const LogStorage * poLogStorage, const IOLoop * poIOLoop,
		   const CheckpointMgr * poCheckpointMgr, const SMFac * poSMFac)
  : Base(poConfig, poMsgTransport, poInstance), m_oLearnerState(poConfig, poLogStorage),
    m_oPaxosLog(poLogStorage), m_oLearnerSender((Config *)poConfig, this, &m_oPaxosLog),
    m_oCheckpointReceiver((Config *)poConfig, (LogStorage *)poLogStorage) {
  m_poAcceptor = (Acceptor *)poAcceptor;
  InitForNewPaxosInstance();

  m_iAskforlearn_noopTimerID = 0;
  m_poIOLoop = (IOLoop *)poIOLoop;

  m_poCheckpointMgr = (CheckpointMgr *)poCheckpointMgr;
  m_poSMFac = (SMFac *)poSMFac;
  m_poCheckpointSender = nullptr;

  m_llHighestSeenInstanceID = 0;
  m_iHighestSeenInstanceID_FromNodeID = nullnode;

  m_bIsIMLearning = false;

  m_llLastAckInstanceID = 0;
}

Learner :: ~Learner(){
  delete m_poCheckpointSender;
}

void Learner :: StartLearnerSender()
{
    m_oLearnerSender.start();
}

const bool Learner :: IsLearned()
{
    return m_oLearnerState.GetIsLearned();
}

const std::string & Learner :: GetLearnValue()
{
    return m_oLearnerState.GetLearnValue();
}

void Learner :: InitForNewPaxosInstance()
{
    m_oLearnerState.Init();
}

const uint32_t Learner :: GetNewChecksum() const
{
    return m_oLearnerState.GetNewChecksum();
}

////////////////////////////////////////////////////////////////

void Learner :: Stop()
{
    m_oLearnerSender.Stop();
    if (m_poCheckpointSender != nullptr)
    {
        m_poCheckpointSender->Stop();
    }
}

////////////////////////////////////////////////////////////////

const bool Learner :: IsIMLatest() 
{
    return (GetInstanceID() + 1) >= m_llHighestSeenInstanceID;
}

const uint64_t Learner :: GetSeenLatestInstanceID()
{
    return m_llHighestSeenInstanceID;
}

void Learner :: SetSeenInstanceID(const uint64_t llInstanceID, const nodeid_t llFromNodeID)
{
    if (llInstanceID > m_llHighestSeenInstanceID)
    {
        m_llHighestSeenInstanceID = llInstanceID;
        m_iHighestSeenInstanceID_FromNodeID = llFromNodeID;
    }
}

//////////////////////////////////////////////////////////////

void Learner :: Reset_AskforLearn_Noop(const int iTimeout)
{
    if (m_iAskforlearn_noopTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(m_iAskforlearn_noopTimerID);
    }

    m_poIOLoop->AddTimer(iTimeout, Timer_Learner_Askforlearn_noop, 0, m_iAskforlearn_noopTimerID);
}

void Learner :: AskforLearn_Noop(const bool bIsStart)
{
    Reset_AskforLearn_Noop();

    m_bIsIMLearning = false;

    m_poCheckpointMgr->ExitCheckpointMode();

    AskforLearn();
    
    if (bIsStart)
    {
        AskforLearn();
    }
}

///////////////////////////////////////////////////////////////

void Learner :: AskforLearn()
{
    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_AskforLearn);

    if (m_poConfig->IsIMFollower())
    {
        //this is not proposal nodeid, just use this val to bring followto nodeid info.
        oPaxosMsg.set_proposalnodeid(m_poConfig->GetFollowToNodeID());
    }

	PAXOSLOG_DEBUG << "END InstanceID " << oPaxosMsg.instanceid() << " MyNodeID " << oPaxosMsg.nodeid();

    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_None, Message_SendType_TCP);
    BroadcastMessageToTempNode(oPaxosMsg, Message_SendType_UDP);
}

void Learner :: OnAskforLearn(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnAskforLearn();

	PAXOSLOG_INFO << "START Msg.InstanceID " << oPaxosMsg.instanceid() << " Now.InstanceID " << GetInstanceID() <<
		" Msg.from_nodeid " << oPaxosMsg.nodeid() << " MinChosenInstanceID " << m_poCheckpointMgr->GetMinChosenInstanceID();
    
    SetSeenInstanceID(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());

    if (oPaxosMsg.proposalnodeid() == m_poConfig->GetMyNodeID())
    {
        //Found a node follow me.
		PAXOSLOG_INFO << "Found a node " << oPaxosMsg.nodeid() << " follow me.";
        m_poConfig->AddFollowerNode(oPaxosMsg.nodeid());
    }
    
    if (oPaxosMsg.instanceid() >= GetInstanceID())
    {
        return;
    }

    if (oPaxosMsg.instanceid() >= m_poCheckpointMgr->GetMinChosenInstanceID())
    {
        if (!m_oLearnerSender.Prepare(oPaxosMsg.instanceid(), oPaxosMsg.nodeid()))
        {
            //BP->GetLearnerBP()->OnAskforLearnGetLockFail();
			PAXOSLOG_ERROR << "LearnerSender working for others.";

            if (oPaxosMsg.instanceid() == (GetInstanceID() - 1))
            {
				PAXOSLOG_INFO << "InstanceID only difference one, just send this value to other.";
                //send one value
                AcceptorStateData oState;
                int ret = m_oPaxosLog.ReadState(m_poConfig->GetMyGroupIdx(), oPaxosMsg.instanceid(), oState);
                if (ret == 0)
                {
                    //BallotNumber oBallot(oState.acceptedid(), oState.acceptednodeid());
                    SendLearnValue(oPaxosMsg.nodeid(), oPaxosMsg.instanceid(), oState.acceptedvalue(), 0, false);
                }
            }
            
            return;
        }
    }
    
    SendNowInstanceID(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());
}

void Learner :: SendNowInstanceID(const uint64_t llInstanceID, const nodeid_t iSendNodeID)
{
    //BP->GetLearnerBP()->SendNowInstanceID();

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_instanceid(llInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendNowInstanceID);
    oPaxosMsg.set_nowinstanceid(GetInstanceID());
    oPaxosMsg.set_minchoseninstanceid(m_poCheckpointMgr->GetMinChosenInstanceID());

    if ((GetInstanceID() - llInstanceID) > 50)
    {
        //instanceid too close not need to send vsm/master checkpoint. 
        string sSystemVariablesCPBuffer;
        int ret = m_poConfig->GetSystemVSM()->GetCheckpointBuffer(sSystemVariablesCPBuffer);
        if (ret == 0)
        {
            oPaxosMsg.set_systemvariables(sSystemVariablesCPBuffer);
        }

        string sMasterVariablesCPBuffer;
        if (m_poConfig->GetMasterSM() != nullptr)
        {
            int ret = m_poConfig->GetMasterSM()->GetCheckpointBuffer(sMasterVariablesCPBuffer);
            if (ret == 0)
            {
                oPaxosMsg.set_mastervariables(sMasterVariablesCPBuffer);
            }
        }
    }

    SendMessage(iSendNodeID, oPaxosMsg);
}

void Learner :: OnSendNowInstanceID(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnSendNowInstanceID();

    //PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.from_nodeid %lu Msg.MaxInstanceID %lu systemvariables_size %zu mastervariables_size %zu",
    //        oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.nodeid(), oPaxosMsg.nowinstanceid(), 
    //        oPaxosMsg.systemvariables().size(), oPaxosMsg.mastervariables().size());

    SetSeenInstanceID(oPaxosMsg.nowinstanceid(), oPaxosMsg.nodeid());

    bool bSystemVariablesChange = false;
    int ret = m_poConfig->GetSystemVSM()->UpdateByCheckpoint(oPaxosMsg.systemvariables(), bSystemVariablesChange);
    if (ret == 0 && bSystemVariablesChange)
    {
		PAXOSLOG_INFO << "SystemVariables changed!, all thing need to reflesh, so skip this msg";
        return;
    }

    bool bMasterVariablesChange = false;
    if (m_poConfig->GetMasterSM() != nullptr)
    {
        ret = m_poConfig->GetMasterSM()->UpdateByCheckpoint(oPaxosMsg.mastervariables(), bMasterVariablesChange);
        if (ret == 0 && bMasterVariablesChange)
        {
			PAXOSLOG_INFO << "MasterVariables changed!";
        }
    }

    if (oPaxosMsg.instanceid() != GetInstanceID())
    {
		PAXOSLOG_ERROR << "Lag msg, skip";
        return;
    }

    if (oPaxosMsg.nowinstanceid() <= GetInstanceID())
    {
        PAXOSLOG_ERROR << "Lag msg, skip";
        return;
    }

    if (oPaxosMsg.minchoseninstanceid() > GetInstanceID())
    {
        //BP->GetCheckpointBP()->NeedAskforCheckpoint();

		PAXOSLOG_INFO << "my instanceid " << GetInstanceID() << " small than other's minchoseninstanceid " << 
			oPaxosMsg.minchoseninstanceid() << ", other nodeid " << oPaxosMsg.nodeid();

        AskforCheckpoint(oPaxosMsg.nodeid());
    }
    else if (!m_bIsIMLearning)
    {
        ComfirmAskForLearn(oPaxosMsg.nodeid());
    }
}

////////////////////////////////////////////

void Learner :: ComfirmAskForLearn(const nodeid_t iSendNodeID)
{
    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_ComfirmAskforLearn);

	PAXOSLOG_INFO << "END InstanceID " << GetInstanceID() << " MyNodeID " << oPaxosMsg.nodeid();

    SendMessage(iSendNodeID, oPaxosMsg);

    m_bIsIMLearning = true;
}

void Learner :: OnComfirmAskForLearn(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnComfirmAskForLearn();
	PAXOSLOG_INFO << "START Msg.InstanceID " << oPaxosMsg.instanceid() << " Msg.from_nodeid " << oPaxosMsg.nodeid();

    if (!m_oLearnerSender.Comfirm(oPaxosMsg.instanceid(), oPaxosMsg.nodeid()))
    {
        //BP->GetLearnerBP()->OnComfirmAskForLearnGetLockFail();
		PAXOSLOG_ERROR << "LearnerSender comfirm fail, maybe is lag msg";
        return;
    }

	PAXOSLOG_INFO << "OK, success comfirm";
}

int Learner :: SendLearnValue(
        const nodeid_t iSendNodeID,
        const uint64_t llLearnInstanceID,
        //const BallotNumber & oLearnedBallot,
        const std::string & sLearnedValue,
        const uint32_t iChecksum,
        const bool bNeedAck)
{
    //BP->GetLearnerBP()->SendLearnValue();
    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue);
    oPaxosMsg.set_instanceid(llLearnInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    //oPaxosMsg.set_proposalnodeid(oLearnedBallot.m_llNodeID);
    //oPaxosMsg.set_proposalid(oLearnedBallot.m_llProposalID);
    oPaxosMsg.set_value(sLearnedValue);
    oPaxosMsg.set_lastchecksum(iChecksum);
    if (bNeedAck)
    {
        oPaxosMsg.set_flag(PaxosMsgFlagType_SendLearnValue_NeedAck);
    }

    return SendMessage(iSendNodeID, oPaxosMsg, Message_SendType_TCP);
}

void Learner :: OnSendLearnValue(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnSendLearnValue();

    //PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.ballot_proposalid %lu Msg.ballot_nodeid %lu Msg.ValueSize %zu",
    //        oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.proposalid(), 
    //        oPaxosMsg.nodeid(), oPaxosMsg.value().size());
	PAXOSLOG_INFO << "===START Msg.InstanceID " << oPaxosMsg.instanceid() << " Now.InstanceID " << GetInstanceID() << " Msg.ballot_nodeid  " 
				<< oPaxosMsg.nodeid() << " Msg.ValueSize " << oPaxosMsg.value().size();
	
    if (oPaxosMsg.instanceid() > GetInstanceID())
    {
		PAXOSLOG_DEBUG << "[Latest Msg] i can't learn";
        return;
    }

    if (oPaxosMsg.instanceid() < GetInstanceID())
    {
		PAXOSLOG_DEBUG << "[Lag Msg] no need to learn";
    }
    else
    {
        //learn value
        //BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.proposalnodeid());
        int ret = m_oLearnerState.LearnValue(oPaxosMsg.instanceid(), oPaxosMsg.nodeid(), oPaxosMsg.value(), GetLastChecksum());
        if (ret != 0)
        {
			PAXOSLOG_ERROR << "LearnState.LearnValue fail, ret " << ret;
            return;
        }

		SetInstanceID(oPaxosMsg.instanceid());
		PAXOSLOG_INFO << "END LearnValue OK, proposalid " << oPaxosMsg.proposalid() << " proposalid_nodeid " << 
			oPaxosMsg.nodeid() << " valueLen " << oPaxosMsg.value().size();
    }

    if (oPaxosMsg.flag() == PaxosMsgFlagType_SendLearnValue_NeedAck)
    {
        //every time' when receive valid need ack learn value, reset noop timeout.
        Reset_AskforLearn_Noop();

        SendLearnValue_Ack(oPaxosMsg.nodeid());
    }
}

void Learner :: SendLearnValue_Ack(const nodeid_t iSendNodeID)
{
    //PLGHead("START LastAck.Instanceid %lu Now.Instanceid %lu", m_llLastAckInstanceID, GetInstanceID());

    if (GetInstanceID() < m_llLastAckInstanceID + LearnerReceiver_ACK_LEAD)
    {
		PAXOSLOG_INFO << "No need to ack";
        return;
    }
    
    //BP->GetLearnerBP()->SendLearnValue_Ack();

    m_llLastAckInstanceID = GetInstanceID();

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue_Ack);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());

    SendMessage(iSendNodeID, oPaxosMsg);

	PAXOSLOG_INFO << "End. ok";
}

void Learner :: OnSendLearnValue_Ack(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnSendLearnValue_Ack();

	PAXOSLOG_INFO << "Msg.Ack.Instanceid " << oPaxosMsg.instanceid() << " Msg.from_nodeid " << oPaxosMsg.nodeid();
    m_oLearnerSender.Ack(oPaxosMsg.instanceid(), oPaxosMsg.nodeid());
}

//////////////////////////////////////////////////////////////

void Learner :: TransmitToFollower()
{
    if (m_poConfig->GetMyFollowerCount() == 0)
    {
        return;
    }
    
    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_SendLearnValue);
    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    //oPaxosMsg.set_proposalnodeid(m_poAcceptor->GetAcceptorState()->GetAcceptedBallot().m_llNodeID);
    //oPaxosMsg.set_proposalid(m_poAcceptor->GetAcceptorState()->GetAcceptedBallot().m_llProposalID);
    //oPaxosMsg.set_value(m_poAcceptor->GetAcceptorState()->GetAcceptedValue());
    oPaxosMsg.set_lastchecksum(GetLastChecksum());

    BroadcastMessageToFollower(oPaxosMsg, Message_SendType_TCP);

    //PLGHead("ok");
}

void Learner :: ProposerSendSuccess(
        const uint64_t llLearnInstanceID,
        const std::string& sValue)
{
    //BP->GetLearnerBP()->ProposerSendSuccess();

    PaxosMsg oPaxosMsg;
    
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_ProposerSendSuccess);
    oPaxosMsg.set_instanceid(llLearnInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
	oPaxosMsg.set_value(sValue);
    //oPaxosMsg.set_proposalid(llProposalID);
    //oPaxosMsg.set_lastchecksum(GetLastChecksum());

    //run self first
    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_First);
}

void Learner :: OnProposerSendSuccess(const PaxosMsg & oPaxosMsg)
{
    //BP->GetLearnerBP()->OnProposerSendSuccess();

    /*PLGHead("START Msg.InstanceID %lu Now.InstanceID %lu Msg.ProposalID %lu State.AcceptedID %lu "
            "State.AcceptedNodeID %lu, Msg.from_nodeid %lu",
            oPaxosMsg.instanceid(), GetInstanceID(), oPaxosMsg.proposalid(), 
            m_poAcceptor->GetAcceptorState()->GetAcceptedBallot().m_llProposalID,
            m_poAcceptor->GetAcceptorState()->GetAcceptedBallot().m_llNodeID, 
            oPaxosMsg.nodeid());*/
#if 0
    if (oPaxosMsg.instanceid() != GetInstanceID())
    {
        //Instance id not same, that means not in the same instance, ignord.
        PAXOSLOG_DEBUG << "InstanceID not same, skip msg";
        return;
    }

    if (m_poAcceptor->GetAcceptorState()->GetAcceptedBallot().isnull())
    {
        //Not accept any yet.
        //BP->GetLearnerBP()->OnProposerSendSuccessNotAcceptYet();
		PAXOSLOG_DEBUG << "I haven't accpeted any proposal";
        return;
    }

    BallotNumber oBallot(oPaxosMsg.proposalid(), oPaxosMsg.nodeid());

    if (m_poAcceptor->GetAcceptorState()->GetAcceptedBallot()
            != oBallot)
    {
        //Proposalid not same, this accept value maybe not chosen value.
		PAXOSLOG_DEBUG << "ProposalBallot not same to AcceptedBallot";
        //BP->GetLearnerBP()->OnProposerSendSuccessBallotNotSame();
        return;
    }

    //learn value.
    m_oLearnerState.LearnValueWithoutWrite(
            oPaxosMsg.instanceid(),
            m_poAcceptor->GetAcceptorState()->GetAcceptedValue(),
            m_poAcceptor->GetAcceptorState()->GetChecksum());
    
    //BP->GetLearnerBP()->OnProposerSendSuccessSuccessLearn();
	PAXOSLOG_INFO << "END Learn value OK, value " << m_poAcceptor->GetAcceptorState()->GetAcceptedValue().size();
#endif
	SetInstanceID(oPaxosMsg.instanceid());
	m_oLearnerState.LearnValueWithoutWrite(
            oPaxosMsg.instanceid(),
            oPaxosMsg.value(),
            0);

	PAXOSLOG_INFO << "END Learn value OK, value " << oPaxosMsg.value().size();
    TransmitToFollower();

}

////////////////////////////////////////////////////////////////////////

void Learner :: AskforCheckpoint(const nodeid_t iSendNodeID)
{
    //PLGHead("START");

    int ret = m_poCheckpointMgr->PrepareForAskforCheckpoint(iSendNodeID);
    if (ret != 0)
    {
        return;
    }

    PaxosMsg oPaxosMsg;

    oPaxosMsg.set_instanceid(GetInstanceID());
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oPaxosMsg.set_msgtype(MsgType_PaxosLearner_AskforCheckpoint);

    PAXOSLOG_INFO << "END InstanceID " << GetInstanceID() << " MyNodeID " << oPaxosMsg.nodeid();
     
    SendMessage(iSendNodeID, oPaxosMsg);
}

void Learner :: OnAskforCheckpoint(const PaxosMsg & oPaxosMsg)
{
    CheckpointSender * poCheckpointSender = GetNewCheckpointSender(oPaxosMsg.nodeid());
    if (poCheckpointSender != nullptr)
    {
        poCheckpointSender->start();
        PAXOSLOG_INFO << "new checkpoint sender started, send to nodeid " << oPaxosMsg.nodeid();
    }
    else
    {
		PAXOSLOG_ERROR << "Checkpoint Sender is running";
    }
}

int Learner :: SendCheckpointBegin(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_BEGIN);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);

	PAXOSLOG_INFO << "END, SendNodeID " << iSendNodeID << " uuid " << llUUID << " sequence " << llSequence <<
			" cpi " << llCheckpointInstanceID;

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: SendCheckpointEnd(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_END);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);

	PAXOSLOG_INFO << "END, SendNodeID " << iSendNodeID << " uuid " << llUUID << " sequence " << llSequence << 
			" cpi " << llCheckpointInstanceID;
    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: SendCheckpoint(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const uint64_t llCheckpointInstanceID,
        const uint32_t iChecksum,
        const std::string & sFilePath,
        const int iSMID,
        const uint64_t llOffset,
        const std::string & sBuffer)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_flag(CheckpointSendFileFlag_ING);
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_checkpointinstanceid(llCheckpointInstanceID);
    oCheckpointMsg.set_checksum(iChecksum);
    oCheckpointMsg.set_filepath(sFilePath);
    oCheckpointMsg.set_smid(iSMID);
    oCheckpointMsg.set_offset(llOffset);
    oCheckpointMsg.set_buffer(sBuffer);

    //PLGImp("END, SendNodeID %lu uuid %lu sequence %lu cpi %lu checksum %u smid %d offset %lu buffsize %zu filepath %s",
    //        iSendNodeID, llUUID, llSequence, llCheckpointInstanceID, 
    //        iChecksum, iSMID, llOffset, sBuffer.size(), sFilePath.c_str());

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

int Learner :: OnSendCheckpoint_Begin(const CheckpointMsg & oCheckpointMsg)
{
    int ret = m_oCheckpointReceiver.NewReceiver(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid());
    if (ret == 0)
    {
		PAXOSLOG_INFO << "NewReceiver ok";

        ret = m_poCheckpointMgr->SetMinChosenInstanceID(oCheckpointMsg.checkpointinstanceid());
        if (ret != 0)
        {
			PAXOSLOG_ERROR << "SetMinChosenInstanceID fail, ret " << ret << " CheckpointInstanceID " <<
						oCheckpointMsg.checkpointinstanceid();

            return ret;
        }
    }

    return ret;
}

int Learner :: OnSendCheckpoint_Ing(const CheckpointMsg & oCheckpointMsg)
{
    //BP->GetCheckpointBP()->OnSendCheckpointOneBlock();
    return m_oCheckpointReceiver.ReceiveCheckpoint(oCheckpointMsg);
}

int Learner :: OnSendCheckpoint_End(const CheckpointMsg & oCheckpointMsg)
{
    if (!m_oCheckpointReceiver.IsReceiverFinish(oCheckpointMsg.nodeid(), 
                oCheckpointMsg.uuid(), oCheckpointMsg.sequence()))
    {
		PAXOSLOG_ERROR << "receive end msg but receiver not finish";
        return -1;
    }
    
    //BP->GetCheckpointBP()->ReceiveCheckpointDone();

    std::vector<StateMachine *> vecSMList = m_poSMFac->GetSMList();
    for (auto & poSM : vecSMList)
    {
        if (poSM->SMID() == SYSTEM_V_SMID
                || poSM->SMID() == MASTER_V_SMID)
        {
            //system variables sm no checkpoint
            //master variables sm no checkpoint
            continue;
        }

        string sTmpDirPath = m_oCheckpointReceiver.GetTmpDirPath(poSM->SMID());
        std::vector<std::string> vecFilePathList;

        int ret = FileUtils :: IterDir(sTmpDirPath, vecFilePathList);
        if (ret != 0)
        {
			PAXOSLOG_ERROR << "IterDir fail, dirpath " << sTmpDirPath;
        }

        if (vecFilePathList.size() == 0)
        {
			PAXOSLOG_INFO << "this sm " << poSM->SMID() << " have no checkpoint";
            continue;
        }
        
        ret = poSM->LoadCheckpointState(
                m_poConfig->GetMyGroupIdx(),
                sTmpDirPath,
                vecFilePathList,
                oCheckpointMsg.checkpointinstanceid());
        if (ret != 0)
        {
            //BP->GetCheckpointBP()->ReceiveCheckpointAndLoadFail();
            return ret;
        }

    }

    //BP->GetCheckpointBP()->ReceiveCheckpointAndLoadSucc();
	PAXOSLOG_INFO << "All sm load state ok, start to exit process";
    exit(-1);

    return 0;
}

void Learner :: OnSendCheckpoint(const CheckpointMsg & oCheckpointMsg)
{
    //PLGHead("START uuid %lu flag %d sequence %lu cpi %lu checksum %u smid %d offset %lu buffsize %zu filepath %s",
    //        oCheckpointMsg.uuid(), oCheckpointMsg.flag(), oCheckpointMsg.sequence(), 
    //        oCheckpointMsg.checkpointinstanceid(), oCheckpointMsg.checksum(), oCheckpointMsg.smid(), 
    //        oCheckpointMsg.offset(), oCheckpointMsg.buffer().size(), oCheckpointMsg.filepath().c_str());

    int ret = 0;
    
    if (oCheckpointMsg.flag() == CheckpointSendFileFlag_BEGIN)
    {
        ret = OnSendCheckpoint_Begin(oCheckpointMsg);
    }
    else if (oCheckpointMsg.flag() == CheckpointSendFileFlag_ING)
    {
        ret = OnSendCheckpoint_Ing(oCheckpointMsg);
    }
    else if (oCheckpointMsg.flag() == CheckpointSendFileFlag_END)
    {
        ret = OnSendCheckpoint_End(oCheckpointMsg);
    }

    if (ret != 0)
    {
		PAXOSLOG_ERROR << "[FAIL] reset checkpoint receiver and reset askforlearn";

        m_oCheckpointReceiver.Reset();

        Reset_AskforLearn_Noop(5000);
        SendCheckpointAck(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), CheckpointSendFileAckFlag_Fail);
    }
    else
    {
        SendCheckpointAck(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence(), CheckpointSendFileAckFlag_OK);
        Reset_AskforLearn_Noop(120000);
    }
}

int Learner :: SendCheckpointAck(
        const nodeid_t iSendNodeID,
        const uint64_t llUUID,
        const uint64_t llSequence,
        const int iFlag)
{
    CheckpointMsg oCheckpointMsg;

    oCheckpointMsg.set_msgtype(CheckpointMsgType_SendFile_Ack);
    oCheckpointMsg.set_nodeid(m_poConfig->GetMyNodeID());
    oCheckpointMsg.set_uuid(llUUID);
    oCheckpointMsg.set_sequence(llSequence);
    oCheckpointMsg.set_flag(iFlag);

    return SendMessage(iSendNodeID, oCheckpointMsg, Message_SendType_TCP);
}

void Learner :: OnSendCheckpointAck(const CheckpointMsg & oCheckpointMsg)
{
	PAXOSLOG_INFO << "START flag " << oCheckpointMsg.flag();

    if (m_poCheckpointSender != nullptr && !m_poCheckpointSender->IsEnd())
    {
        if (oCheckpointMsg.flag() == CheckpointSendFileAckFlag_OK)
        {
            m_poCheckpointSender->Ack(oCheckpointMsg.nodeid(), oCheckpointMsg.uuid(), oCheckpointMsg.sequence());
        }
        else
        {
            m_poCheckpointSender->End();
        }
    }
}

CheckpointSender * Learner :: GetNewCheckpointSender(const nodeid_t iSendNodeID)
{
    if (m_poCheckpointSender != nullptr)
    {
        if (m_poCheckpointSender->IsEnd())
        {
            m_poCheckpointSender->join();
            delete m_poCheckpointSender;
            m_poCheckpointSender = nullptr;
        }
    }

    if (m_poCheckpointSender == nullptr)
    {
        m_poCheckpointSender = new CheckpointSender(iSendNodeID, m_poConfig, this, m_poSMFac, m_poCheckpointMgr);
        return m_poCheckpointSender;
    }

    return nullptr;
}


}

