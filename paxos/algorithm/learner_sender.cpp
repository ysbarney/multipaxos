/*
 * Module_name: learner_sender.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "learner_sender.h"
#include "learner.h"

namespace multipaxos
{

LearnerSender :: LearnerSender(Config * poConfig, Learner * poLearner, PaxosLog * poPaxosLog)
    : m_poConfig(poConfig), m_poLearner(poLearner), m_poPaxosLog(poPaxosLog)
{
    m_iAckLead = LearnerSender_ACK_LEAD; 
    m_bIsEnd = false;
    m_bIsStart = false;
    SendDone();
}

LearnerSender :: ~LearnerSender()
{
}

void LearnerSender :: Stop()
{
    if (m_bIsStart)
    {
        m_bIsEnd = true;
        join();
    }
}

void LearnerSender :: run()
{
    m_bIsStart = true;

    while (true)
    {
        WaitToSend();

        if (m_bIsEnd)
        {
            PAXOSLOG_DEBUG << "Learner.Sender [END]";
            return;
        }

        SendLearnedValue(m_llBeginInstanceID, m_iSendToNodeID);

        SendDone();
    }
}

////////////////////////////////////////

void LearnerSender :: ReleshSending()
{
    m_llAbsLastSendTime = Time::GetSteadyClockMS();
}

const bool LearnerSender :: IsIMSending()
{
    if (!m_bIsIMSending)
    {
        return false;
    }

    uint64_t llNowTime = Time::GetSteadyClockMS();
    uint64_t llPassTime = llNowTime > m_llAbsLastSendTime ? llNowTime - m_llAbsLastSendTime : 0;

    if ((int)llPassTime >= LearnerSender_PREPARE_TIMEOUT)
    {
        return false;
    }

    return true;
}

void LearnerSender :: CutAckLead()
{
    int iReceiveAckLead = LearnerReceiver_ACK_LEAD;
    if (m_iAckLead - iReceiveAckLead > iReceiveAckLead)
    {
        m_iAckLead = m_iAckLead - iReceiveAckLead;
    }
}

const bool LearnerSender :: CheckAck(const uint64_t llSendInstanceID)
{
    m_oLock.Lock();

    if (llSendInstanceID < m_llAckInstanceID)
    {
        m_iAckLead = LearnerSender_ACK_LEAD;
		PAXOSLOG_INFO << "Already catch up, ack instanceid " << m_llAckInstanceID << " now send instanceid "
				<< llSendInstanceID;
        m_oLock.UnLock();
        return false;
    }

    while (llSendInstanceID > m_llAckInstanceID + m_iAckLead)
    {
        uint64_t llNowTime = Time::GetSteadyClockMS();
        uint64_t llPassTime = llNowTime > m_llAbsLastAckTime ? llNowTime - m_llAbsLastAckTime : 0;

        if ((int)llPassTime >= LearnerSender_ACK_TIMEOUT)
        {
            //BP->GetLearnerBP()->SenderAckTimeout();
			PAXOSLOG_ERROR << "Ack timeout, last acktime " << m_llAbsLastAckTime << " now send instanceid "
					<< llSendInstanceID;
            CutAckLead();
            m_oLock.UnLock();
            return false;
        }

        //BP->GetLearnerBP()->SenderAckDelay();
        //PLGErr("Need sleep to slow down send speed, sendinstaceid %lu ackinstanceid %lu",
                //llSendInstanceID, m_llAckInstanceID);
        
        m_oLock.WaitTime(20);
    }

    m_oLock.UnLock();

    return true;
}

//////////////////////////////////////////////////////////////////////////

const bool LearnerSender :: Prepare(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID)
{
    m_oLock.Lock();
    
    bool bPrepareRet = false;
    if (!IsIMSending() && !m_bIsComfirmed)
    {
        bPrepareRet = true;

        m_bIsIMSending = true;
        m_llAbsLastSendTime = m_llAbsLastAckTime = Time::GetSteadyClockMS();
        m_llBeginInstanceID = m_llAckInstanceID = llBeginInstanceID;
        m_iSendToNodeID = iSendToNodeID;
    }
    
    m_oLock.UnLock();

    return bPrepareRet;
}

const bool LearnerSender :: Comfirm(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID)
{
    m_oLock.Lock();

    bool bComfirmRet = false;

    if (IsIMSending() && (!m_bIsComfirmed))
    {
        if (m_llBeginInstanceID == llBeginInstanceID && m_iSendToNodeID == iSendToNodeID)
        {
            bComfirmRet = true;

            m_bIsComfirmed = true;
            m_oLock.Interupt();
        }
    }

    m_oLock.UnLock();

    return bComfirmRet;
}

void LearnerSender :: Ack(const uint64_t llAckInstanceID, const nodeid_t iFromNodeID)
{
    m_oLock.Lock();

    if (IsIMSending() && m_bIsComfirmed)
    {
        if (m_iSendToNodeID == iFromNodeID)
        {
            if (llAckInstanceID > m_llAckInstanceID)
            {
                m_llAckInstanceID = llAckInstanceID;
                m_llAbsLastAckTime = Time::GetSteadyClockMS();
                m_oLock.Interupt();
            }
        }
    }

    m_oLock.UnLock();
}    

///////////////////////////////////////////////

void LearnerSender :: WaitToSend()
{
    m_oLock.Lock();
    while (!m_bIsComfirmed)
    {
        m_oLock.WaitTime(1000);
        if (m_bIsEnd)
        {
            break;
        }
    }
    m_oLock.UnLock();
}

void LearnerSender :: SendLearnedValue(const uint64_t llBeginInstanceID, const nodeid_t iSendToNodeID)
{
	PAXOSLOG_DEBUG << "BeginInstanceID " << llBeginInstanceID << " SendToNodeID " << iSendToNodeID;
    uint64_t llSendInstanceID = llBeginInstanceID;
    int ret = 0;
    
    uint32_t iLastChecksum = 0;

    //control send speed to avoid affecting the network too much.
    int iSendQps = LearnerSender_SEND_QPS;
    int iSleepMs = iSendQps > 1000 ? 1 : 1000 / iSendQps;
    int iSendInterval = iSendQps > 1000 ? iSendQps / 1000 + 1 : 1; 

	PAXOSLOG_DEBUG << "SendQps " << iSendQps << " SleepMs " << iSleepMs << " SendInterval " << iSendInterval
				 	<< " AckLead " << m_iAckLead;
    int iSendCount = 0;
    while (llSendInstanceID < m_poLearner->GetInstanceID())
    {    
        ret = SendOne(llSendInstanceID, iSendToNodeID, iLastChecksum);
        if (ret != 0)
        {
			PAXOSLOG_ERROR << "SendOne fail, SendInstanceID " << llSendInstanceID <<" SendToNodeID " << iSendToNodeID
					<< " ret " << ret;
            return;
        }

        if (!CheckAck(llSendInstanceID))
        {
            return;
        }

        iSendCount++;
        llSendInstanceID++;
        ReleshSending();

        if (iSendCount >= iSendInterval)
        {
            iSendCount = 0;
            Time::MsSleep(iSleepMs);
        }
    }

    //succ send, reset ack lead.
    m_iAckLead = LearnerSender_ACK_LEAD;

	PAXOSLOG_INFO << "SendDone, SendEndInstanceID " << llSendInstanceID;
}

int LearnerSender :: SendOne(const uint64_t llSendInstanceID, const nodeid_t iSendToNodeID, uint32_t & iLastChecksum)
{
    //BP->GetLearnerBP()->SenderSendOnePaxosLog();

    AcceptorStateData oState;
    int ret = m_poPaxosLog->ReadState(m_poConfig->GetMyGroupIdx(), llSendInstanceID, oState);
    if (ret != 0)
    {
        return ret;
    }

    BallotNumber oBallot(oState.acceptedid(), oState.acceptednodeid());

    ret = m_poLearner->SendLearnValue(iSendToNodeID, llSendInstanceID, oBallot, oState.acceptedvalue(), iLastChecksum);

    iLastChecksum = oState.checksum();

    return ret;
}

void LearnerSender :: SendDone()
{
    m_oLock.Lock();

    m_bIsIMSending = false;
    m_bIsComfirmed = false;
    m_llBeginInstanceID = (uint64_t)-1;
    m_iSendToNodeID = nullnode;
    m_llAbsLastSendTime = 0;
    
    m_llAckInstanceID = 0;
    m_llAbsLastAckTime = 0;

    m_oLock.UnLock();
}

    
}

