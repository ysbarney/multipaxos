/*
 * Module_name: proposer.h
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#pragma once
 
#include "base.h"
#include <string>
#include "ioloop.h"
#include "msg_counter.h"
#include "learner.h"
 
namespace multipaxos
{
class ProposerState
{
public:
	ProposerState(const Config * poConfig, const std::string& sValue);
	virtual ~ProposerState();

	TimeStat& get_TimeStat()
	{
		return m_oTimeStat;
	}

	MsgCounter& get_MsgCounter()
	{
		return m_oMsgCounter;
	}

	const std::string& get_Value() const
	{
		return m_sValue;
	}

	uint32_t get_PrepareTimerID()
	{
		return m_iPrepareTimerID;
	}
	void set_PrepareTimerID(uint32_t iPrepareTimerID)
	{
		m_iPrepareTimerID = iPrepareTimerID;
	}

	int get_LastPrepareTimeoutMs()
	{
		return m_iLastPrepareTimeoutMs;
	}
	void set_LastPrepareTimeoutMs(int iLastPrepareTimeoutMs)
	{
		m_iLastPrepareTimeoutMs = iLastPrepareTimeoutMs;
	}

	uint32_t get_AcceptTimerID()
	{
		return m_iAcceptTimerID;
	}
	void set_AcceptTimerID(uint32_t iAcceptTimerID)
	{
		m_iAcceptTimerID = iAcceptTimerID;
	}

	int get_LastAcceptTimeoutMs()
	{
		return m_iLastAcceptTimeoutMs;
	}
	void set_LastAcceptTimeoutMs(int iLastAcceptTimeoutMs)
	{
		m_iLastAcceptTimeoutMs = iLastAcceptTimeoutMs;
	}

	uint64_t get_llTimeoutInstanceID()
	{
		return m_llTimeoutInstanceID;
	}
	void set_llTimeoutInstanceID(uint64_t llTimeoutInstanceID)
	{
		m_llTimeoutInstanceID = llTimeoutInstanceID;
	}

	bool get_IsPreparing()
	{
		return m_bIsPreparing;
	}
	void set_IsPreparing(bool bIsPreparing)
	{
		m_bIsPreparing = bIsPreparing;
	}

	bool get_IsAccepting()
	{
		return m_bIsAccepting;
	}
	void set_IsAccepting(bool bIsAccepting)
	{
		m_bIsAccepting = bIsAccepting;
	}
private:
	TimeStat m_oTimeStat;
	MsgCounter m_oMsgCounter;
	std::string m_sValue;
	uint32_t m_iPrepareTimerID;
	int m_iLastPrepareTimeoutMs;
	uint32_t m_iAcceptTimerID;
	int m_iLastAcceptTimeoutMs;
	uint64_t m_llTimeoutInstanceID;
	bool m_bIsPreparing;
	bool m_bIsAccepting;
};

class Learner;

class Proposer : public Base , public gensequence
{
public:
	 Proposer(
			 const Config * poConfig, 
			 const MsgTransport * poMsgTransport,
			 const Instance * poInstance,
			 const Learner * poLearner,
			 const IOLoop * poIOLoop);
	 ~Proposer();

	 int NewValue(const std::string & sValue);
	 
	 bool IsWorking();

	 /////////////////////////////

	 void Prepare(const uint64_t llInstanceID);

	 void OnPrepareReply(const PaxosMsg & oPaxosMsg);

	 void OnExpiredPrepareReply(const PaxosMsg & oPaxosMsg);

	 void Accept(const uint64_t llInstanceID);

	 void OnAcceptReply(const PaxosMsg & oPaxosMsg);

	 void OnExpiredAcceptReply(const PaxosMsg & oPaxosMsg);

	 void OnPrepareTimeout(const uint64_t llInstanceID);

	 void OnAcceptTimeout(const uint64_t llInstanceID);

	 void ExitPrepare(const uint64_t llInstanceID);

	 void ExitAccept(const uint64_t llInstanceID);

	 void CancelSkipPrepare();

	 /////////////////////////////

	 void AddPrepareTimer(const uint64_t llInstanceID, const int iTimeoutMs = 0);
	 
	 void AddAcceptTimer(const uint64_t llInstanceID, const int iTimeoutMs = 0);

	 void SafeProValueMapDel(const uint64_t llInstanceID);
public:
	 Learner * m_poLearner;
	 Config * m_poConfig;

	 IOLoop * m_poIOLoop;

	 bool m_bCanSkipPrepare;

	 bool m_bWasRejectBySomeone;

	 std::map<uint64_t, ProposerState*> m_proValueMap;
};

class QueueProposer : public Thread
{
public:
	QueueProposer(Proposer*       oProposer);
	virtual ~QueueProposer();

	void run();

	int AddMessage(const char * pcMessage, const int iMessageLen);
	
private:
	Proposer *m_oProposer;
	concurrent_queue<std::string *> m_oMessageQueue;
	int m_iQueueMemSize;
};

}

