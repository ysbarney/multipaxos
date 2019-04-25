/*
 * Module_name: cleaner.cpp
 * Author: Barneyliu
 * Time: 2019-01-09
 * Description:
 *
 */

#include "cleaner.h"
#include "phxpaxos/storage.h"
#include "comm_include.h"
#include "config_include.h"
#include "cp_mgr.h"
#include "sm_base.h"

namespace multipaxos
{

Cleaner :: Cleaner(
    Config * poConfig, 
    SMFac * poSMFac, 
    LogStorage * poLogStorage, 
    CheckpointMgr * poCheckpointMgr)
    : m_poConfig(poConfig), 
    m_poSMFac(poSMFac), 
    m_poLogStorage(poLogStorage), 
    m_poCheckpointMgr(poCheckpointMgr),
    m_llLastSave(0),
    m_bCanrun(false),
    m_bIsPaused(true),
    m_bIsEnd(false),
    m_bIsStart(false),
    m_llHoldCount(CAN_DELETE_DELTA)
{
}

Cleaner :: ~Cleaner()
{
}

void Cleaner :: Stop()
{
    m_bIsEnd = true;
    if (m_bIsStart)
    {
        join();
    }
}

void Cleaner :: Pause()
{
    m_bCanrun = false;
}

void Cleaner :: Continue()
{
    m_bIsPaused = false;
    m_bCanrun = true;
}

const bool Cleaner :: IsPaused() const
{
    return m_bIsPaused;
}

void Cleaner :: run()
{
    m_bIsStart = true;
    Continue();

    //control delete speed to avoid affecting the io too much.
    int iDeleteQps = Cleaner_DELETE_QPS;
    int iSleepMs = iDeleteQps > 1000 ? 1 : 1000 / iDeleteQps;
    int iDeleteInterval = iDeleteQps > 1000 ? iDeleteQps / 1000 + 1 : 1; 

	PAXOSLOG_DEBUG << "DeleteQps " << iDeleteQps << " SleepMs " << iSleepMs
			<< " DeleteInterval " << iDeleteInterval;
    while (true)
    {
        if (m_bIsEnd)
        {
			PAXOSLOG_DEBUG << "Checkpoint.Cleaner [END]";
            return;
        }
        
        if (!m_bCanrun)
        {
			PAXOSLOG_INFO << "Pausing, sleep";
            m_bIsPaused = true;
            Time::MsSleep(1000);
            continue;
        }

        uint64_t llInstanceID = m_poCheckpointMgr->GetMinChosenInstanceID();
        uint64_t llCPInstanceID = m_poSMFac->GetCheckpointInstanceID(m_poConfig->GetMyGroupIdx()) + 1;
        uint64_t llMaxChosenInstanceID = m_poCheckpointMgr->GetMaxChosenInstanceID();

        int iDeleteCount = 0;
        while ((llInstanceID + m_llHoldCount < llCPInstanceID)
                && (llInstanceID + m_llHoldCount < llMaxChosenInstanceID))
        {
            bool bDeleteRet = DeleteOne(llInstanceID);
            if (bDeleteRet)
            {
                //PLGImp("delete one done, instanceid %lu", llInstanceID);
                llInstanceID++;
                iDeleteCount++;
                if (iDeleteCount >= iDeleteInterval)
                {
                    iDeleteCount = 0;
                    Time::MsSleep(iSleepMs);
                }
            }
            else
            {
				PAXOSLOG_DEBUG << "delete system fail, instanceid " << llInstanceID;
                break;
            }
        }

        if (llCPInstanceID == 0)
        {
			PAXOSLOG_INFO << "sleep a while, max deleted instanceid " << llInstanceID 
					<< " checkpoint instanceid (no checkpoint) now instanceid " << m_poCheckpointMgr->GetMaxChosenInstanceID();
        }
        else
        {
			PAXOSLOG_INFO << "sleep a while, max deleted instanceid " << llInstanceID << " checkpoint instanceid " << llCPInstanceID
					<< " now instanceid " << m_poCheckpointMgr->GetMaxChosenInstanceID();
        }

        Time::MsSleep(OtherUtils::FastRand() % 500 + 500);
    }
}

int Cleaner :: FixMinChosenInstanceID(const uint64_t llOldMinChosenInstanceID)
{
    uint64_t llCPInstanceID = m_poSMFac->GetCheckpointInstanceID(m_poConfig->GetMyGroupIdx()) + 1;
    uint64_t llFixMinChosenInstanceID = llOldMinChosenInstanceID;
    int ret = 0;

    for (uint64_t llInstanceID = llOldMinChosenInstanceID; llInstanceID < llOldMinChosenInstanceID + DELETE_SAVE_INTERVAL;
           llInstanceID++)    
    {
        if (llInstanceID >= llCPInstanceID)
        {
            break;
        }
        
        std::string sValue;
        ret = m_poLogStorage->Get(m_poConfig->GetMyGroupIdx(), llInstanceID, sValue);
        if (ret != 0 && ret != 1)
        {
            return -1;
        }
        else if (ret == 1)
        {
            llFixMinChosenInstanceID = llInstanceID + 1;
        }
        else
        {
            break;
        }
    }
    
    if (llFixMinChosenInstanceID > llOldMinChosenInstanceID)
    {
        ret = m_poCheckpointMgr->SetMinChosenInstanceID(llFixMinChosenInstanceID);
        if (ret != 0)
        {
            return ret;
        }
    }

	PAXOSLOG_INFO << "ok, old minchosen " << llOldMinChosenInstanceID << " fix minchosen " << llFixMinChosenInstanceID;
    return 0;
}

bool Cleaner :: DeleteOne(const uint64_t llInstanceID)
{
    WriteOptions oWriteOptions;
    oWriteOptions.bSync = false;

    int ret = m_poLogStorage->Del(oWriteOptions, m_poConfig->GetMyGroupIdx(), llInstanceID);
    if (ret != 0)
    {
        return false;
    }

    m_poCheckpointMgr->SetMinChosenInstanceIDCache(llInstanceID);

    if (llInstanceID >= m_llLastSave + DELETE_SAVE_INTERVAL)
    {
        int ret = m_poCheckpointMgr->SetMinChosenInstanceID(llInstanceID + 1);
        if (ret != 0)
        {
			PAXOSLOG_ERROR << "SetMinChosenInstanceID fail, now delete instanceid " << llInstanceID;
            return false;
        }

        m_llLastSave = llInstanceID;

		PAXOSLOG_INFO << "delete " << DELETE_SAVE_INTERVAL << " instance done, now minchosen instanceid "
			<< llInstanceID + 1;
    }

    return true;
}

void Cleaner :: SetHoldPaxosLogCount(const uint64_t llHoldCount)
{
    if (llHoldCount < 300)
    {
        m_llHoldCount = 300;
    }
    else
    {
        m_llHoldCount = llHoldCount;
    }
}

}

