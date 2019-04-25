/*
 * Module_name: dfnetwork.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "dfnetwork.h"
#include "udp.h"

namespace multipaxos 
{

DFNetWork :: DFNetWork() : m_oUDPRecv(this), m_oTcpIOThread(this)
{
}

DFNetWork :: ~DFNetWork()
{
	PAXOSLOG_DEBUG << "NetWork Deleted!";
}

void DFNetWork :: StopNetWork()
{
    m_oUDPRecv.Stop();
    m_oUDPSend.Stop();
    m_oTcpIOThread.Stop();
}

int DFNetWork :: Init(const std::string & sListenIp, const int iListenPort, const int iIOThreadCount) 
{
	std::string addr = sListenIp+":"+;
    int ret = m_oUDPSend.Init();
    if (ret != 0)
    {
        return ret;
    }

    ret = m_oUDPRecv.Init(iListenPort);
    if (ret != 0)
    {
        return ret;
    }

    ret = m_oTcpIOThread.Init(sListenIp, iListenPort, iIOThreadCount);
    if (ret != 0)
    {
		PAXOSLOG_ERROR << "m_oTcpIOThread Init fail, ret " << ret;
        return ret;
    }

    return 0;
}

void DFNetWork :: RunNetWork()
{
    //m_oUDPSend.start();
    //m_oUDPRecv.start();
    m_oTcpIOThread.Start();
}

int DFNetWork :: SendMessageTCP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage)
{
    return m_oTcpIOThread.AddMessage(iGroupIdx, sIp, iPort, sMessage);
}

int DFNetWork :: SendMessageUDP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage)
{
    return m_oUDPSend.AddMessage(sIp, iPort, sMessage);
}

}


