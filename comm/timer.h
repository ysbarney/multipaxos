/*
 * Module_name: timer.h
 * Author: Barneyliu
 * Time: 2019-01-09
 * Description:
 *
 */

#pragma once

#include <vector>
#include <inttypes.h>

namespace multipaxos
{

class Timer
{
public:
    Timer();
    ~Timer();

    void AddTimer(const uint64_t llAbsTime, const uint64_t llInstanceID, uint32_t & iTimerID);
    
    void AddTimerWithType(const uint64_t llAbsTime, const int iType, const uint64_t llInstanceID,uint32_t & iTimerID);

    bool PopTimeout(uint32_t & iTimerID, int & iType, uint64_t& llInstanceID);

    const int GetNextTimeout() const;
    
private:
    struct TimerObj
    {
        TimerObj(uint32_t iTimerID, uint64_t llAbsTime, int iType, uint64_t llInstanceID) 
            : m_iTimerID(iTimerID), m_llAbsTime(llAbsTime), m_iType(iType), m_llInstanceID(llInstanceID) {}

        uint32_t m_iTimerID;
        uint64_t m_llAbsTime;
        int m_iType;
		uint64_t m_llInstanceID;

        bool operator < (const TimerObj & obj) const
        {
            if (obj.m_llAbsTime == m_llAbsTime)
            {
                return obj.m_iTimerID < m_iTimerID;
            }
            else
            {
                return obj.m_llAbsTime < m_llAbsTime;
            }
        }
    };

private:
    uint32_t m_iNowTimerID;
    std::vector<TimerObj> m_vecTimerHeap;
};
    
}

