/*
 * Module_name: node.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "node.h"
#include "pnode.h"

namespace multipaxos
{

int Node :: RunNode(const Options & oOptions, Node *& poNode)
{
    if (oOptions.bIsLargeValueMode)
    {
        InsideOptions::Instance()->SetAsLargeBufferMode();
    }
    
    InsideOptions::Instance()->SetGroupCount(oOptions.iGroupCount);
        
    poNode = nullptr;
    NetWork * poNetWork = nullptr;
	NetWork * poExtraNetWork = nullptr;
    //Breakpoint::m_poBreakpoint = nullptr;
    //BP->SetInstance(oOptions.poBreakpoint);

    PNode * poRealNode = new PNode();
    int ret = poRealNode->Init(oOptions, poNetWork, poExtraNetWork);
    if (ret != 0)
    {
        delete poRealNode;
        return ret;
    }

    //step1 set node to network
    //very important, let network on recieve callback can work.
    poNetWork->m_poNode = poRealNode;
	poExtraNetWork->m_poNode = poRealNode;
    //step2 run network.
    //start recieve message from network, so all must init before this step.
    //must be the last step.
    poNetWork->RunNetWork();
	poExtraNetWork->RunNetWork();

    poNode = poRealNode;

    return 0;
}
    
}

