/*
 * Module_name: network.h
 * Author: Barneyliu
 * Time: 2018-12-21
 * Description:
 *
 */

#pragma once

#include <string>
#include <typeinfo>
#include <inttypes.h>

namespace multipaxos
{

//You can use your own network to make paxos communicate. :)

class Node;

class NetWork
{
public:
    NetWork();
    virtual ~NetWork() {}

    //Network must not send/recieve any message before paxoslib called this funtion.
    virtual void RunNetWork() = 0;

    //If paxoslib call this function, network need to stop receive any message.
    virtual void StopNetWork() = 0;

    virtual int SendMessageTCP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage) = 0;

    virtual int SendMessageUDP(const int iGroupIdx, const std::string & sIp, const int iPort, const std::string & sMessage) = 0;

    //When receive a message, call this funtion.
    //This funtion is async, just enqueue an return.
    int OnReceiveMessage(const unsigned short iport, const char * pcMessage, const int iMessageLen);
private:
    friend class Node;
    Node * m_poNode;
};
    
}

