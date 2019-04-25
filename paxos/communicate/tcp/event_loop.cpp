/*
Tencent is pleased to support the open source community by making 
PhxPaxos available.
Copyright (C) 2016 THL A29 Limited, a Tencent company. 
All rights reserved.

Licensed under the BSD 3-Clause License (the "License"); you may 
not use this file except in compliance with the License. You may 
obtain a copy of the License at

https://opensource.org/licenses/BSD-3-Clause

Unless required by applicable law or agreed to in writing, software 
distributed under the License is distributed on an "AS IS" basis, 
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or 
implied. See the License for the specific language governing 
permissions and limitations under the License.

See the AUTHORS file for names of contributors. 
*/

#include "event_loop.h"
#include "event_base.h"
#include "tcp_acceptor.h"
#include "tcp_client.h"
#include "comm_include.h"
#include "message_event.h"
#include "network.h"

using namespace std;

namespace multipaxos
{

TEventLoop :: TEventLoop(NetWork * poNetWork)
{
    m_iEpollFd = -1;
    m_bIsEnd = false;
    m_poNetWork = poNetWork;
    m_poTcpClient = nullptr;
    m_poNotify = nullptr;
    memset(m_EpollEvents, 0, sizeof(m_EpollEvents));
}

TEventLoop :: ~TEventLoop()
{
    ClearEvent();
}

void TEventLoop :: JumpoutEpollWait()
{
    m_poNotify->SendNotify();
}

void TEventLoop :: SetTcpClient(TcpClient * poTcpClient)
{
    m_poTcpClient = poTcpClient;
}

int TEventLoop :: Init(const int iEpollLength)
{
    m_iEpollFd = epoll_create(iEpollLength);
    if (m_iEpollFd == -1)
    {
		PAXOSLOG_ERROR << "epoll_create fail, ret " << m_iEpollFd;
        return -1;
    }

    m_poNotify = new Notify(this);
    assert(m_poNotify != nullptr);
    
    int ret = m_poNotify->Init();
    if (ret != 0)
    {
        return ret;
    }

    return 0;
}

void TEventLoop :: ModEvent(const Event * poEvent, const int iEvents)
{
    auto it = m_mapEvent.find(poEvent->GetSocketFd());
    int iEpollOpertion = 0;
    if (it == end(m_mapEvent))
    {
        iEpollOpertion = EPOLL_CTL_ADD;
    }
    else
    {
        iEpollOpertion = it->second.m_iEvents ? EPOLL_CTL_MOD : EPOLL_CTL_ADD;
    }

    epoll_event tEpollEvent;
    tEpollEvent.events = iEvents;
    tEpollEvent.data.fd = poEvent->GetSocketFd();

    int ret = epoll_ctl(m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd(), &tEpollEvent);
    if (ret == -1)
    {
        PAXOSLOG_ERROR << "epoll_ctl fail, EpollFd " << m_iEpollFd << " EpollOpertion "<< iEpollOpertion
				<< " SocketFd " << poEvent->GetSocketFd() << " EpollEvent " << iEvents;
        //to do 
        return;
    }

    EventCtx tCtx;
    tCtx.m_poEvent = (Event *)poEvent;
    tCtx.m_iEvents = iEvents;
    
    m_mapEvent[poEvent->GetSocketFd()] = tCtx;
}

void TEventLoop :: RemoveEvent(const Event * poEvent)
{
    auto it = m_mapEvent.find(poEvent->GetSocketFd());
    if (it == end(m_mapEvent))
    {
        return;
    }

    int iEpollOpertion = EPOLL_CTL_DEL;

    epoll_event tEpollEvent;
    tEpollEvent.events = 0;
    tEpollEvent.data.fd = poEvent->GetSocketFd();

    int ret = epoll_ctl(m_iEpollFd, iEpollOpertion, poEvent->GetSocketFd(), &tEpollEvent);
    if (ret == -1)
    {
		PAXOSLOG_ERROR << "epoll_ctl fail, EpollFd " << m_iEpollFd << " EpollOpertion " << iEpollOpertion
				<< " SocketFd " << poEvent->GetSocketFd();
        //to do 
        //when error
        return;
    }

    m_mapEvent.erase(poEvent->GetSocketFd());
}

void TEventLoop :: StartLoop()
{
    m_bIsEnd = false;
    while(true)
    {
        //BP->GetNetworkBP()->TcpEpollLoop();

        int iNextTimeout = 1000;
        
        DealwithTimeout(iNextTimeout);

        //PLHead("nexttimeout %d", iNextTimeout);

        OneLoop(iNextTimeout);

        CreateEvent();

        if (m_poTcpClient != nullptr)
        {
            m_poTcpClient->DealWithWrite();
        }

        if (m_bIsEnd)
        {
			PAXOSLOG_DEBUG << "TCP.TEventLoop [END]";
            break;
        }
    }
}

void TEventLoop :: Stop()
{
    m_bIsEnd = true;
}

void TEventLoop :: OneLoop(const int iTimeoutMs)
{
    int n = epoll_wait(m_iEpollFd, m_EpollEvents, MAX_EVENTS, 1);
    if (n == -1)
    {
        if (errno != EINTR)
        {
			PAXOSLOG_ERROR << "epoll_wait fail, errno " << errno;
            return;
        }
    }

    for (int i = 0; i < n; i++)
    {
        int iFd = m_EpollEvents[i].data.fd;
        auto it = m_mapEvent.find(iFd);
        if (it == end(m_mapEvent))
        {
            continue;
        }

        int iEvents = m_EpollEvents[i].events;
        Event * poEvent = it->second.m_poEvent;

        int ret = 0;
        if (iEvents & EPOLLERR)
        {
            OnError(iEvents, poEvent);
            continue;
        }
        
        try
        {
            if (iEvents & EPOLLIN)
            {
                ret = poEvent->OnRead();
            }

            if (iEvents & EPOLLOUT)
            {
                ret = poEvent->OnWrite();
            }
        }
        catch (...)
        {
            ret = -1;
        }

        if (ret != 0)
        {
            OnError(iEvents, poEvent);
        }
    }
}

void TEventLoop :: OnError(const int iEvents, Event * poEvent)
{
    //BP->GetNetworkBP()->TcpOnError();

    //PLErr("event error, events %d socketfd %d socket ip %s errno %d", 
    //        iEvents, poEvent->GetSocketFd(), poEvent->GetSocketHost().c_str(), errno);

    RemoveEvent(poEvent);

    bool bNeedDelete = false;
    poEvent->OnError(bNeedDelete);
    
    if (bNeedDelete)
    {
        poEvent->Destroy();
    }
}

bool TEventLoop :: AddTimer(const Event * poEvent, const int iTimeout, const int iType, uint32_t & iTimerID)
{
    if (poEvent->GetSocketFd() == 0)
    {
        return false;
    }    

    if (m_mapEvent.find(poEvent->GetSocketFd()) == end(m_mapEvent))
    {
        EventCtx tCtx;
        tCtx.m_poEvent = (Event *)poEvent;
        tCtx.m_iEvents = 0;

        m_mapEvent[poEvent->GetSocketFd()] = tCtx;
    }

    uint64_t llAbsTime = Time::GetSteadyClockMS() + iTimeout;
    m_oTimer.AddTimerWithType(llAbsTime, iType, 0, iTimerID);

    m_mapTimerID2FD[iTimerID] = poEvent->GetSocketFd();

    return true;
}

void TEventLoop :: RemoveTimer(const uint32_t iTimerID)
{
    auto it = m_mapTimerID2FD.find(iTimerID);
    if (it != end(m_mapTimerID2FD))
    {
        m_mapTimerID2FD.erase(it);
    }
}

void TEventLoop :: DealwithTimeoutOne(const uint32_t iTimerID, const int iType)
{
    auto it = m_mapTimerID2FD.find(iTimerID);
    if (it == end(m_mapTimerID2FD))
    {
        //PLErr("Timeout aready remove!, timerid %u iType %d", iTimerID, iType);
        return;
    }

    int iSocketFd = it->second;

    m_mapTimerID2FD.erase(it);

    auto eventIt = m_mapEvent.find(iSocketFd);
    if (eventIt == end(m_mapEvent))
    {
        return;
    }

    eventIt->second.m_poEvent->OnTimeout(iTimerID, iType);
}

void TEventLoop :: DealwithTimeout(int & iNextTimeout)
{
    bool bHasTimeout = true;

    while(bHasTimeout)
    {
        uint32_t iTimerID = 0;
        int iType = 0;
		uint64_t llInstanceID = 0;
        bHasTimeout = m_oTimer.PopTimeout(iTimerID, iType, llInstanceID);

        if (bHasTimeout)
        {
            DealwithTimeoutOne(iTimerID, iType);

            iNextTimeout = m_oTimer.GetNextTimeout();
            if (iNextTimeout != 0)
            {
                break;
            }
        }
    }
}

void TEventLoop :: AddEvent(int iFD, SocketAddress oAddr)
{
    std::lock_guard<std::mutex> oLockGuard(m_oMutex);
    m_oFDQueue.push(make_pair(iFD, oAddr));
}

void TEventLoop :: CreateEvent()
{
    std::lock_guard<std::mutex> oLockGuard(m_oMutex);

    if (m_oFDQueue.empty())
    {
        return;
    }

    ClearEvent();
    
    int iCreatePerTime = 200;
    while ((!m_oFDQueue.empty()) && iCreatePerTime--)
    {
        auto oData = m_oFDQueue.front();
        m_oFDQueue.pop();

        //create event for this fd
        MessageEvent * poMessageEvent = new MessageEvent(MessageEventType_RECV, oData.first, 
                oData.second, this, m_poNetWork);
        poMessageEvent->AddEvent(EPOLLIN);

        m_vecCreatedEvent.push_back(poMessageEvent);
    }
}

void TEventLoop :: ClearEvent()
{
    for (auto it = m_vecCreatedEvent.begin(); it != end(m_vecCreatedEvent);)
    {
        if ((*it)->IsDestroy())
        {
            delete (*it);
            it = m_vecCreatedEvent.erase(it);
        }
        else
        {
            it++;
        }
    }
}

int TEventLoop :: GetActiveEventCount()
{
    std::lock_guard<std::mutex> oLockGuard(m_oMutex);
    ClearEvent();
    return (int)m_vecCreatedEvent.size();
}

}


