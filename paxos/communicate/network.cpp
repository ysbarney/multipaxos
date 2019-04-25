/*
 * Module_name: network.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "network.h"
#include "node.h"
#include "commdef.h"

namespace multipaxos
{

NetWork :: NetWork() : m_poNode(nullptr)
{
}
    
int NetWork :: OnReceiveMessage(const unsigned short iport, const char * pcMessage, const int iMessageLen)
{
    if (m_poNode != nullptr)
    {
        m_poNode->OnReceiveMessage(iport ,pcMessage, iMessageLen);
    }
    else
    {
		PAXOSLOG_DEBUG << "receive msglen " << iMessageLen;
    }

    return 0;
}

}


