/*
 * Module_name: timer.cpp
 * Author: Barneyliu
 * Time: 2019-01-09
 * Description:
 *
 */
 
#include "timer.h"
#include "util.h"
#include <algorithm> 

namespace multipaxos
{

Timer :: Timer() : m_iNowTimerID(1)
{
}

Timer :: ~Timer()
{
}

void Timer :: AddTimer(const uint64_t llAbsTime, const uint64_t llInstanceID, uint32_t & iTimerID)
{
	return AddTimerWithType(llAbsTime, 0, llInstanceID, iTimerID);
}

void Timer :: AddTimerWithType(const uint64_t llAbsTime, const int iType, const uint64_t llInstanceID, uint32_t & iTimerID)
{
	iTimerID = m_iNowTimerID++;

	TimerObj tObj(iTimerID, llAbsTime, iType, llInstanceID);
	m_vecTimerHeap.push_back(tObj);
	push_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
}

const int Timer :: GetNextTimeout() const
{
	if (m_vecTimerHeap.empty())
	{
		return -1;
	}

	int iNextTimeout = 0;

	TimerObj tObj = m_vecTimerHeap.front();
	uint64_t llNowTime = Time::GetSteadyClockMS();
	if (tObj.m_llAbsTime > llNowTime)
	{
		iNextTimeout = (int)(tObj.m_llAbsTime - llNowTime);
	}

	return iNextTimeout;
}

bool Timer :: PopTimeout(uint32_t & iTimerID, int & iType, uint64_t& llInstanceID)
{
	if (m_vecTimerHeap.empty())
	{
		return false;
	}

	TimerObj tObj = m_vecTimerHeap.front();
	uint64_t llNowTime = Time::GetSteadyClockMS();
	if (tObj.m_llAbsTime > llNowTime)
	{
		return false;
	}
	
	pop_heap(begin(m_vecTimerHeap), end(m_vecTimerHeap));
	m_vecTimerHeap.pop_back();

	iTimerID = tObj.m_iTimerID;
	iType = tObj.m_iType;
	llInstanceID = tObj.m_llInstanceID;

	return true;
}	 
	
}
