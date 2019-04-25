/*
 * Module_name: sm.cpp
 * Author: Barneyliu
 * Time: 2019-01-09
 * Description:
 *
 */

#include "sm.h"
#include "comm_include.h"

namespace multipaxos
{

SMCtx :: SMCtx(const int iSMID, void * pCtx) : m_iSMID(iSMID), m_pCtx(pCtx)
{
}

SMCtx :: SMCtx() : m_iSMID(0), m_pCtx(nullptr)
{
}

bool StateMachine :: ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, 
        const std::string & sPaxosValue) 
{ 
    return true; 
}

const uint64_t StateMachine :: GetCheckpointInstanceID(const int iGroupIdx) const 
{ 
    return multipaxos::NoCheckpoint;
}

//default no checkpoint 
int StateMachine :: GetCheckpointState(const int iGroupIdx, std::string & sDirPath, 
        std::vector<std::string> & vecFileList) 
{ 
	PAXOSLOG_ERROR << "func not impl, return -1";
    return -1; 
}    

int StateMachine :: LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
        const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID) 
{ 
	PAXOSLOG_ERROR << "func not impl, return -1";
    return -1;
}

int StateMachine :: LockCheckpointState() 
{ 
    PAXOSLOG_ERROR << "func not impl, return -1";
    return -1; 
}

void StateMachine :: UnLockCheckpointState() 
{ 
}

void StateMachine :: BeforePropose(const int iGroupIdx, std::string & sValue)
{
}

const bool StateMachine :: NeedCallBeforePropose()
{
    return false;
}

}

