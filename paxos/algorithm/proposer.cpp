/*
 * Module_name: proposer.cpp
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#include "proposer.h"

namespace multipaxos
{

ProposerState::ProposerState(const Config * poConfig, const std :: string & sValue)
	: m_oMsgCounter(poConfig), m_sValue(sValue)
{
	m_iPrepareTimerID = 0;
    m_iAcceptTimerID = 0;
    m_llTimeoutInstanceID = 0;

	m_bIsPreparing = false;
    m_bIsAccepting = false;

    m_iLastPrepareTimeoutMs = poConfig->GetPrepareTimeoutMs();
    m_iLastAcceptTimeoutMs = poConfig->GetAcceptTimeoutMs();
}

ProposerState::~ProposerState()
{}

Proposer :: Proposer(
        const Config * poConfig, 
        const MsgTransport * poMsgTransport,
        const Instance * poInstance,
        const Learner * poLearner,
        const IOLoop * poIOLoop)
    : Base(poConfig, poMsgTransport, poInstance)
{
    m_poLearner = (Learner *)poLearner;
    m_poIOLoop = (IOLoop *)poIOLoop;
	m_poConfig = (Config *)poConfig;
	
    m_bCanSkipPrepare = false;

    m_bWasRejectBySomeone = false;
}

Proposer :: ~Proposer()
{
}

int Proposer :: NewValue(const std::string & sValue)
{
	uint64_t llInstanceID = GenInstanceID();
	PAXOSLOG_INFO << "NewValue llInstanceID " << llInstanceID;
	try {
		ProposerState* oProposerState = new ProposerState(m_poConfig, sValue);
		if(m_proValueMap.find(llInstanceID) != m_proValueMap.end()) {
			PAXOSLOG_ERROR << "llInstanceID " << llInstanceID << " is in the m_proValueMap, please check";
			return -1;
		}
		
		m_proValueMap.insert(std::make_pair(llInstanceID, oProposerState));
		oProposerState->set_LastPrepareTimeoutMs(START_PREPARE_TIMEOUTMS);
		oProposerState->set_LastAcceptTimeoutMs(START_ACCEPT_TIMEOUTMS);
	} catch(std::exception& e) {
		PAXOSLOG_ERROR << "===NewValue new failed " << e.what();
		return -1;
	}
	
    if (m_bCanSkipPrepare && !m_bWasRejectBySomeone)
    {
		PAXOSLOG_INFO << "skip prepare, directly start accept";
		Accept(llInstanceID);
    }
    else
    {
        //if not reject by someone, no need to increase ballot
        Prepare(llInstanceID);
    }

    return 0;
}

void Proposer :: Prepare(const uint64_t llInstanceID)
{
    m_proValueMap[llInstanceID]->get_TimeStat().Point();
    
    ExitAccept(llInstanceID);
    m_proValueMap[llInstanceID]->set_IsPreparing(true);
    m_bCanSkipPrepare = false;
    m_bWasRejectBySomeone = false;

    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosPrepare);
    oPaxosMsg.set_instanceid(llInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    //oPaxosMsg.set_proposalid(llInstanceID);

	m_proValueMap[llInstanceID]->get_MsgCounter().StartNewRound();
	
    AddPrepareTimer(llInstanceID);

    BroadcastMessage(oPaxosMsg);
}

void Proposer :: OnPrepareReply(const PaxosMsg & oPaxosMsg)
{
	//PAXOSLOG_INFO << "===OnPrepareReply Now.InstanceID " << oPaxosMsg.instanceid() << " MsgType " << oPaxosMsg.msgtype() << " Msg.from_nodeid " << oPaxosMsg.nodeid();
    if(m_proValueMap.find(oPaxosMsg.instanceid()) == m_proValueMap.end()) {
		PAXOSLOG_ERROR << "OnPrepareReply Propose nodeid "<< oPaxosMsg.nodeid() << " llInstanceID " << oPaxosMsg.instanceid() <<" not same, skip this msg";
        return;
	}

	ProposerState *curProposeStat = m_proValueMap[oPaxosMsg.instanceid()];
	if(!curProposeStat) {
		PAXOSLOG_ERROR << "OnPrepareReply m_proValueMap llInstanceID " << oPaxosMsg.instanceid() << " is NULL";
		return;
	}

	if(!curProposeStat->get_IsPreparing()) 
		return;

    curProposeStat->get_MsgCounter().AddReceive(oPaxosMsg.nodeid());

    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        curProposeStat->get_MsgCounter().AddPromiseOrAccept(oPaxosMsg.nodeid());
    }
    else
    {
		PAXOSLOG_DEBUG << "[Reject] RejectByPromiseID " << oPaxosMsg.rejectbypromiseid() << " from nodeid " << oPaxosMsg.nodeid();
        curProposeStat->get_MsgCounter().AddReject(oPaxosMsg.nodeid());
        m_bWasRejectBySomeone = true;
    }

    if (curProposeStat->get_MsgCounter().IsPassedOnThisRound())
    {
        int iUseTimeMs = curProposeStat->get_TimeStat().Point();
		PAXOSLOG_INFO << "[Pass] start accept, usetime " << iUseTimeMs << "ms";
        m_bCanSkipPrepare = true;
        Accept(oPaxosMsg.instanceid());
    }
    else if (curProposeStat->get_MsgCounter().IsRejectedOnThisRound()
            || curProposeStat->get_MsgCounter().IsAllReceiveOnThisRound())
    {
        PAXOSLOG_INFO << "[Not Pass] wait 30ms and restart prepare";
        AddPrepareTimer(OtherUtils::FastRand() % 30 + 10);
    }
}

void Proposer :: OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg)
{
    if (oPaxosMsg.rejectbypromiseid() != 0)
    {
		PAXOSLOG_DEBUG << "[Expired Prepare Reply Reject] RejectByPromiseID " << oPaxosMsg.rejectbypromiseid();
		m_bWasRejectBySomeone = true;
    }
}

void Proposer :: Accept(const uint64_t llInstanceID)
{
    m_proValueMap[llInstanceID]->get_TimeStat().Point();
    
    ExitPrepare(llInstanceID);
    //m_bIsAccepting = true;
    m_proValueMap[llInstanceID]->set_IsAccepting(true);
    
    PaxosMsg oPaxosMsg;
    oPaxosMsg.set_msgtype(MsgType_PaxosAccept);
    oPaxosMsg.set_instanceid(llInstanceID);
    oPaxosMsg.set_nodeid(m_poConfig->GetMyNodeID());
    //oPaxosMsg.set_proposalid(m_oProposerState.GetProposalID());
    oPaxosMsg.set_value(m_proValueMap[llInstanceID]->get_Value());
    oPaxosMsg.set_lastchecksum(GetLastChecksum());

    m_proValueMap[llInstanceID]->get_MsgCounter().StartNewRound();

    AddAcceptTimer(llInstanceID);
	
    BroadcastMessage(oPaxosMsg, BroadcastMessage_Type_RunSelf_Final);
}

void Proposer :: OnAcceptReply(const PaxosMsg & oPaxosMsg)
{
	//PAXOSLOG_INFO << "===OnAcceptReply Now.InstanceID " << oPaxosMsg.instanceid() << " MsgType " << oPaxosMsg.msgtype() << " Msg.from_nodeid " << oPaxosMsg.nodeid();
	if(m_proValueMap.find(oPaxosMsg.instanceid()) == m_proValueMap.end()) {
		PAXOSLOG_ERROR << "OnAcceptReply Propose nodeid " << oPaxosMsg.nodeid() << " llInstanceID " << oPaxosMsg.instanceid()
					<< " not same, skip this msg";
        return;
	}

	ProposerState *curProposeStat = m_proValueMap[oPaxosMsg.instanceid()];
	if(!curProposeStat) {
		PAXOSLOG_ERROR << "OnAcceptReply m_proValueMap llInstanceID " << oPaxosMsg.instanceid() << " is NULL";
		return;
	}

	if(!curProposeStat->get_IsAccepting())
		return;

	curProposeStat->get_MsgCounter().AddReceive(oPaxosMsg.nodeid());
	
    if (oPaxosMsg.rejectbypromiseid() == 0)
    {
        PAXOSLOG_DEBUG << "[Accept] nodeid " << oPaxosMsg.nodeid() << " llInstanceID " << oPaxosMsg.instanceid();
        curProposeStat->get_MsgCounter().AddPromiseOrAccept(oPaxosMsg.nodeid());
    }
    else
    {
        PAXOSLOG_DEBUG << "[Reject] nodeid " << oPaxosMsg.nodeid() << " llInstanceID " << oPaxosMsg.instanceid();
		curProposeStat->get_MsgCounter().AddReject(oPaxosMsg.nodeid());
		
        m_bWasRejectBySomeone = true;
    }

    if (curProposeStat->get_MsgCounter().IsPassedOnThisRound())
    {
        int iUseTimeMs = curProposeStat->get_TimeStat().Point();

		PAXOSLOG_INFO << "[Pass] Start send learn, usetime " << iUseTimeMs << "ms";
        ExitAccept(oPaxosMsg.instanceid());
		std::string learnValue = curProposeStat->get_Value();
		SafeProValueMapDel(oPaxosMsg.instanceid());
        m_poLearner->ProposerSendSuccess(oPaxosMsg.instanceid(), learnValue);
    }
    else if (curProposeStat->get_MsgCounter().IsRejectedOnThisRound()
            || curProposeStat->get_MsgCounter().IsAllReceiveOnThisRound())
    {
		PAXOSLOG_INFO << "[Not pass] wait 30ms and Restart prepare";
        AddAcceptTimer(OtherUtils::FastRand() % 30 + 10);
    }

}

void Proposer :: OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg)
{
    if (oPaxosMsg.rejectbypromiseid() != 0)
    {
		PAXOSLOG_DEBUG << "[Expired Accept Reply Reject] RejectByPromiseID " << oPaxosMsg.rejectbypromiseid();
        m_bWasRejectBySomeone = true;
    }
}

void Proposer :: ExitPrepare(const uint64_t llInstanceID)
{
	if(m_proValueMap.find(llInstanceID) == m_proValueMap.end())
		return;
	
    if (m_proValueMap[llInstanceID]->get_IsPreparing())
    {
        m_proValueMap[llInstanceID]->set_IsPreparing(false);
		
        m_poIOLoop->RemoveTimer(m_proValueMap[llInstanceID]->get_PrepareTimerID());
    }
}

void Proposer :: ExitAccept(const uint64_t llInstanceID)
{
	if(m_proValueMap.find(llInstanceID) == m_proValueMap.end())
		return;
	
    if (m_proValueMap[llInstanceID]->get_IsAccepting())
    {
        m_proValueMap[llInstanceID]->set_IsAccepting(false);
        m_poIOLoop->RemoveTimer(m_proValueMap[llInstanceID]->get_AcceptTimerID());
    }
}

void Proposer :: AddPrepareTimer(const uint64_t llInstanceID, const int iTimeoutMs)
{
	uint32_t iPrepareTimerID = m_proValueMap[llInstanceID]->get_PrepareTimerID();
	int iLastPrepareTimeoutMs = m_proValueMap[llInstanceID]->get_LastPrepareTimeoutMs();
	
    if (iPrepareTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(iPrepareTimerID);
    }

    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Prepare_Timeout,
                llInstanceID,
                iPrepareTimerID);
        return;
    }

    m_poIOLoop->AddTimer(
            iLastPrepareTimeoutMs,
            Timer_Proposer_Prepare_Timeout,
            llInstanceID,
            iPrepareTimerID);

	m_proValueMap[llInstanceID]->set_PrepareTimerID(iPrepareTimerID);
    m_proValueMap[llInstanceID]->set_llTimeoutInstanceID(llInstanceID);

    iLastPrepareTimeoutMs *= 2;
    if (iLastPrepareTimeoutMs > MAX_PREPARE_TIMEOUTMS)
    {
        iLastPrepareTimeoutMs = MAX_PREPARE_TIMEOUTMS;
		m_proValueMap[llInstanceID]->set_LastPrepareTimeoutMs(iLastPrepareTimeoutMs);
    }
}

void Proposer :: AddAcceptTimer(const uint64_t llInstanceID, const int iTimeoutMs)
{
	uint32_t iAcceptTimerID = m_proValueMap[llInstanceID]->get_AcceptTimerID();
	int iLastAcceptTimeoutMs = m_proValueMap[llInstanceID]->get_LastAcceptTimeoutMs();
	
    if (iAcceptTimerID > 0)
    {
        m_poIOLoop->RemoveTimer(iAcceptTimerID);
    }

    if (iTimeoutMs > 0)
    {
        m_poIOLoop->AddTimer(
                iTimeoutMs,
                Timer_Proposer_Accept_Timeout,
                llInstanceID,
                iAcceptTimerID);
        return;
    }

    m_poIOLoop->AddTimer(
            iLastAcceptTimeoutMs,
            Timer_Proposer_Accept_Timeout,
            llInstanceID,
            iAcceptTimerID);

    //m_llTimeoutInstanceID = GetInstanceID();
    m_proValueMap[llInstanceID]->set_llTimeoutInstanceID(llInstanceID);

    iLastAcceptTimeoutMs *= 2;
    if (iLastAcceptTimeoutMs > MAX_ACCEPT_TIMEOUTMS)
    {
        iLastAcceptTimeoutMs = MAX_ACCEPT_TIMEOUTMS;
		m_proValueMap[llInstanceID]->set_LastAcceptTimeoutMs(iLastAcceptTimeoutMs);
    }
}

void Proposer :: OnPrepareTimeout(const uint64_t llInstanceID)
{
	if(m_proValueMap.find(llInstanceID) == m_proValueMap.end())
		return;
	
    if (m_proValueMap[llInstanceID]->get_llTimeoutInstanceID() != llInstanceID)
    {
		PAXOSLOG_ERROR << "TimeoutInstanceID " << m_proValueMap[llInstanceID]->get_llTimeoutInstanceID() 
				<< " not same to NowInstanceID " << llInstanceID << ", skip";
		return;
    }
    
    //Prepare(llInstanceID);
    SafeProValueMapDel(llInstanceID);
}

void Proposer :: OnAcceptTimeout(const uint64_t llInstanceID)
{
	if(m_proValueMap.find(llInstanceID) == m_proValueMap.end())
		return;
	
    if (m_proValueMap[llInstanceID]->get_llTimeoutInstanceID() != llInstanceID)
    {
        PAXOSLOG_ERROR << "TimeoutInstanceID " << m_proValueMap[llInstanceID]->get_llTimeoutInstanceID() 
				<< " not same to NowInstanceID " << llInstanceID << ", skip";
        return;
    }
    
    //Prepare(llInstanceID);
    SafeProValueMapDel(llInstanceID);
}

void Proposer :: CancelSkipPrepare()
{
    m_bCanSkipPrepare = false;
}

void Proposer :: SafeProValueMapDel(const uint64_t llInstanceID)
{
	//PAXOSLOG_INFO << "SafeProValueMapDel llInstanceID " << llInstanceID;
	if(m_proValueMap.find(llInstanceID) == m_proValueMap.end()) {
		PAXOSLOG_ERROR << "SafeProValueMapDel can not find llInstanceID " << llInstanceID << " in the m_ProValueMap";
		return;
	}

	delete (m_proValueMap[llInstanceID]);
	m_proValueMap.erase(llInstanceID);
}

/////////////////////////////////////////////////////////////////////
QueueProposer::QueueProposer(
			Proposer* oProposer) : m_oProposer(oProposer)
{
	m_iQueueMemSize = 0;
}

QueueProposer::~QueueProposer()
{
}

void QueueProposer::run()
{
	while(true) {
		std::string * psMessage = nullptr;

		bool bSucc = m_oMessageQueue.peek(psMessage, 1000);
		if(bSucc) {
			if (psMessage != nullptr && psMessage->size() > 0)
	        {
	            m_iQueueMemSize -= psMessage->size();
	            m_oProposer->NewValue(*psMessage);
	        }

	        delete psMessage;
		}
	}
}

int QueueProposer::AddMessage(const char* pcMessage, const int iMessageLen)
{
	if(!pcMessage || iMessageLen <= 0) {
		PAXOSLOG_ERROR << "QueuePropose AddMessage is wrong, skip msg";
		return -1;
	}
	
	while(true) {
		if ((int)m_oMessageQueue.size() > QUEUE_MAXLENGTH)
	    {
			PAXOSLOG_ERROR << "Queue full, skip msg";
			usleep(1000 * 20);   //20ms
	        continue;
	    }

	    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE)
	    {
	        PAXOSLOG_ERROR << "queue memsize " << m_iQueueMemSize << " too large, can't enqueue";
			usleep(1000 * 20); // 20ms
	        continue;
	    }
		break;
	}
	
	m_oMessageQueue.add(new string(pcMessage, iMessageLen));

    m_iQueueMemSize += iMessageLen;
	return 0;
}

}
