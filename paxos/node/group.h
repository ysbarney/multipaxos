/*
 * Module_name: group.h
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#pragma once

#include <thread>
#include "comm_include.h"
#include "config_include.h"
#include "instance.h"
#include "cleaner.h"
#include "communicate.h"
#include "options.h"
#include "network.h"

namespace multipaxos
{

class Group
{
public:
    Group(const Node * poPaxosNode, LogStorage * poLogStorage, 
            NetWork * poNetWork,    
            InsideSM * poMasterSM,
            const int iGroupIdx,
            const Options & oOptions);

    ~Group();

    void StartInit();

    void Init();

    int GetInitRet();

    void Start();

    void Stop();

    Config * GetConfig();

    Instance * GetInstance();

    Cleaner * GetCheckpointCleaner();

    Replayer * GetCheckpointReplayer();

    void AddStateMachine(StateMachine * poSM);

private:
	Communicate m_oCommunicate;
    Config m_oConfig;
    Instance m_oInstance;
	
    int m_iInitRet;
    std::thread * m_poThread;
};
    
}

