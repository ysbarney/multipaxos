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

#include "message_event.h"
#include "commdef.h"
#include "network.h"
#include "event_loop.h"
#include "comm_include.h"

namespace multipaxos
{

MessageEvent :: MessageEvent(
        const int iType,
        const int fd, 
        const SocketAddress & oAddr, 
        TEventLoop * poEventLoop,
        NetWork * poNetWork) :
    Event(poEventLoop), m_oSocket(fd), m_oAddr(oAddr), m_poNetWork(poNetWork)
{
    m_iType = iType;

    m_iLeftReadLen = 0;
    m_iLastReadPos = 0;

    m_iLeftWriteLen = 0;
    m_iLastWritePos = 0;

    memset(m_sReadHeadBuffer, 0, sizeof(m_sReadHeadBuffer));
    m_iLastReadHeadPos = 0;

    m_oSocket.setNonBlocking(true);
    m_oSocket.setNoDelay(true);

    m_iReconnectTimeoutID = 0;

    m_llLastActiveTime = Time::GetSteadyClockMS();
    m_sHost = oAddr.getHost();

    m_iQueueMemSize = 0;
}

MessageEvent :: ~MessageEvent()
{
    while (!m_oInQueue.empty())
    {
        //QueueData tData = m_oInQueue.front();
        //m_oInQueue.pop();
        QueueData tData;
		if(m_oInQueue.peek(tData))
        	delete tData.psValue;
    }
}

int MessageEvent :: GetSocketFd() const
{
    return m_oSocket.getSocketHandle();
}

const std::string & MessageEvent :: GetSocketHost()
{
    return m_sHost;
}

const bool MessageEvent :: IsActive()
{
    uint64_t llNowTime = Time::GetSteadyClockMS();
    if (llNowTime > m_llLastActiveTime
            && ((int)(llNowTime - m_llLastActiveTime) > CONNECTTION_NONACTIVE_TIMEOUT))
    {
        return false;
    }
    
    return true;
}

int MessageEvent :: AddMessage(const std::string & sMessage)
{
    m_llLastActiveTime = Time::GetSteadyClockMS();
    std::unique_lock<std::mutex> oLock(m_oMutex);

	while(true) {
	    if ((int)m_oInQueue.size() > TCP_QUEUE_MAXLEN)
	    {
	        //BP->GetNetworkBP()->TcpQueueFull();
	        PAXOSLOG_ERROR << "queue length " << m_oInQueue.size() << " too long, can't enqueue";
	        usleep(1000 * 20);
			continue;
	    }

	    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE)
	    {
	        PAXOSLOG_ERROR << "queue memsize " << m_iQueueMemSize << " too large, can't enqueue";
	        usleep(1000 * 20);
			continue;
	    }
		break;
	}

    QueueData tData;
    tData.llEnqueueAbsTime = Time::GetSteadyClockMS();
    tData.psValue = new string(sMessage);
	m_oInQueue.add(tData);
    //m_oInQueue.push(tData);

    m_iQueueMemSize += sMessage.size();

    oLock.unlock();

    JumpoutEpollWait();

    return 0;
}

void MessageEvent :: ReadDone(BytesBuffer & oBytesBuffer, const int iLen)
{
    //PLHead("ok, len %d", iLen);
    //PAXOSLOG_INFO << "Recv socket fd " << m_oSocket.getSocketHandle();
    m_poNetWork->OnReceiveMessage(m_oSocket.getLocalAddress().getPort(), oBytesBuffer.GetPtr(), iLen);

    //BP->GetNetworkBP()->TcpReadOneMessageOk(iLen);
}

int MessageEvent :: ReadLeft()
{
    bool bAgain = false;
    int iReadLen = m_oSocket.receive(m_oReadCacheBuffer.GetPtr() + m_iLastReadPos, m_iLeftReadLen, &bAgain);
    //PLImp("readlen %d", iReadLen);
    if (iReadLen == 0)
    {
        //socket broken
        return -1;
    }

    m_iLeftReadLen -= iReadLen;
    m_iLastReadPos += iReadLen;

    if (m_iLeftReadLen == 0)
    {
        ReadDone(m_oReadCacheBuffer, m_iLastReadPos);
        m_iLeftReadLen = 0;
        m_iLastReadPos = 0;
    }

    return 0;
}

int MessageEvent :: OnRead()
{
    if (m_iLeftReadLen > 0)
    {
        return ReadLeft();
    }
    
    int iReadLen = m_oSocket.receive(m_sReadHeadBuffer + m_iLastReadHeadPos, sizeof(int) - m_iLastReadHeadPos);
    if (iReadLen == 0)
    {
        //BP->GetNetworkBP()->TcpOnReadMessageLenError();
		PAXOSLOG_ERROR << "read head fail, readlen " << iReadLen << ", socket broken";
        return -1;
    }

    m_iLastReadHeadPos += iReadLen;
    if (m_iLastReadHeadPos < (int)sizeof(int))
    {
		PAXOSLOG_INFO << "head read pos " << m_iLastReadHeadPos << " small than sizeof(int) " << sizeof(int);
        return 0;
    }
    
    m_iLastReadHeadPos = 0;
    int niLen = 0;
    int iLen = 0;
    memcpy((char *)&niLen, m_sReadHeadBuffer, sizeof(int));
    iLen = ntohl(niLen) - 4;
    
    if (iLen < 0 || iLen > MAX_VALUE_SIZE)
    {
		PAXOSLOG_ERROR << "need to read len wrong " << iLen;
        return -2; 
    }

    m_oReadCacheBuffer.Ready(iLen);

    m_iLeftReadLen = iLen;
    m_iLastReadPos = 0;
    
    //second read maybe no data read, so readlen == 0 is ok.
    bool bAgain = false;
    iReadLen = m_oSocket.receive(m_oReadCacheBuffer.GetPtr(), iLen, &bAgain);
    if (iReadLen == 0)
    {
        if (!bAgain)
        {
			PAXOSLOG_ERROR << "second read data fail, readlen " << iReadLen << ", no again, socket broken";
            return -1;
        }
        else
        {
			PAXOSLOG_ERROR << "second read data, readlen " << iReadLen << " need again";
            return 0;
        }
    }

    if (iReadLen == iLen)
    {
        ReadDone(m_oReadCacheBuffer, iLen);
        m_iLeftReadLen = 0;
        m_iLastReadPos = 0;
    }
    else if (iReadLen < iLen)
    {
        m_iLastReadPos = iReadLen;
        m_iLeftReadLen = iLen - iReadLen;

		PAXOSLOG_INFO << "read buflen " << iReadLen << " small than except len " << iLen;
    }
    else
    {
		PAXOSLOG_ERROR << "read buflen " << iReadLen << " large than except len " << iLen;
        return -2;
    }

    return 0;
}

void MessageEvent :: OpenWrite()
{
    if (!m_oInQueue.empty())
    {
        if (IsDestroy())
        {
			PAXOSLOG_ERROR << "this connection fd:" << GetSocketFd() << " host:" << GetSocketHost() 
						<< " is destroy, reconnect";
            ReConnect();
            m_bIsDestroy = false;
        }
        else
        {
            AddEvent(EPOLLOUT);
        }
    }
}

void MessageEvent :: WriteDone()
{
    //PLHead("ok");
    RemoveEvent(EPOLLOUT);
}

int MessageEvent :: WriteLeft()
{
    int iWriteLen = m_oSocket.send(m_oWriteCacheBuffer.GetPtr() + m_iLastWritePos, m_iLeftWriteLen);
    //PLImp("writelen %d", iWriteLen);
    if (iWriteLen < 0)
    {
        return -1; 
    }

    if (iWriteLen == 0)
    {
        //no buffer to write, wait next epoll_wait
        //need wait next write
        AddEvent(EPOLLOUT);

        return 1;
    }

    m_iLeftWriteLen -= iWriteLen;
    m_iLastWritePos += iWriteLen;

    if (m_iLeftWriteLen == 0)
    {
        m_iLeftWriteLen = 0;
        m_iLastWritePos = 0;
    }

    return 0;
}

int MessageEvent :: OnWrite()
{
    int ret = 0;
    while (!m_oInQueue.empty() || m_iLeftWriteLen > 0)
    {
        ret = DoOnWrite();
        if (ret != 0 && ret != 1)
        {
            return ret;
        }
        else if (ret == 1)
        {
            //need break, wait next write
            return 0;
        }
    }

    WriteDone();

    return 0;
}

int MessageEvent :: DoOnWrite()
{
    if (m_iLeftWriteLen > 0)
    {
        return WriteLeft();
    }

    /*m_oMutex.lock();
    if (m_oInQueue.empty())
    {
        m_oMutex.unlock();
        return 0;
    }
    QueueData tData = m_oInQueue.front();
    m_oInQueue.pop();
    m_iQueueMemSize -= tData.psValue->size();
    m_oMutex.unlock();*/
    QueueData tData;
	bool bSucc = m_oInQueue.peek(tData);
	if(!bSucc) 
		return 0;
	
    std::string * poMessage = tData.psValue;
    uint64_t llNowTime = Time::GetSteadyClockMS();
    int iDelayMs = llNowTime > tData.llEnqueueAbsTime ? (int)(llNowTime - tData.llEnqueueAbsTime) : 0;
    //BP->GetNetworkBP()->TcpOutQueue(iDelayMs);
    if (iDelayMs > TCP_OUTQUEUE_DROP_TIMEMS)
    {
        PAXOSLOG_ERROR << "drop request because enqueue timeout, nowtime " << llNowTime << " unqueuetime "
        		<< tData.llEnqueueAbsTime;
        delete poMessage;
        return 0;
    }

    int iBuffLen = poMessage->size();
    int niBuffLen = htonl(iBuffLen + 4);

    int iLen = iBuffLen + 4;
    m_oWriteCacheBuffer.Ready(iLen);
    memcpy(m_oWriteCacheBuffer.GetPtr(), &niBuffLen, 4);
    memcpy(m_oWriteCacheBuffer.GetPtr() + 4, poMessage->c_str(), iBuffLen);

    m_iLeftWriteLen = iLen;
    m_iLastWritePos = 0;

    delete poMessage;

    //PAXOSLOG_INFO << "write len " << iLen << " ip " << m_oAddr.getHost() << " port " <<  m_oAddr.getPort();

    int iWriteLen = m_oSocket.send(m_oWriteCacheBuffer.GetPtr(), iLen);
    if (iWriteLen < 0)
    {
		PAXOSLOG_ERROR << "fail, write len " << iWriteLen << " ip " << m_oAddr.getHost() << " port " 
					<< m_oAddr.getPort();
        return -1;
    }

    if (iWriteLen == 0)
    {
        //need wait next write
        AddEvent(EPOLLOUT);

        return 1;
    }

    //PLImp("real write len %d", iWriteLen);

    if (iWriteLen == iLen)
    {
        m_iLeftWriteLen = 0;
        m_iLastWritePos = 0;
        //write done
    }
    else if (iWriteLen < iLen)
    {
        m_iLastWritePos = iWriteLen;
        m_iLeftWriteLen = iLen - iWriteLen;

		PAXOSLOG_INFO << "write buflen " << iWriteLen << " smaller than expectlen " << iLen;
    }
    else
    {
		PAXOSLOG_ERROR << "write buflen " << iWriteLen << " large than expectlen " << iLen;
    }

    return 0;
}

void MessageEvent :: OnError(bool & bNeedDelete)
{
    bNeedDelete = false;

    if (m_iType == MessageEventType_RECV)
    {
        bNeedDelete = true;
        return;
    }
    else if (m_iType == MessageEventType_SEND)
    {
        if (IsActive())
        {
            AddTimer(200, MessageEventTimerType_Reconnect, m_iReconnectTimeoutID);
        }
        else
        {
			PAXOSLOG_ERROR << "this connection " << GetSocketHost();

            m_oMutex.lock();

            while (!m_oInQueue.empty())
            {
                //QueueData tData = m_oInQueue.front();
                //m_oInQueue.pop();
                QueueData tData;
				if(m_oInQueue.peek(tData))
                	delete tData.psValue;
            }

            m_iQueueMemSize = 0;

            m_oMutex.unlock();

            bNeedDelete = true;
            return;
        }
    }
}

void MessageEvent :: OnTimeout(const uint32_t iTimerID, const int iType)
{
    if (iTimerID != m_iReconnectTimeoutID)
    {
        return;
    }

    if (iType == MessageEventTimerType_Reconnect)
    {
        ReConnect();
    }
}    

void MessageEvent :: ReConnect()
{
    //BP->GetNetworkBP()->TcpReconnect();

    //reset 
    m_iEvents = 0;
    m_iLeftWriteLen = 0;
    m_iLastWritePos = 0;

    m_oSocket.reset();
    m_oSocket.setNonBlocking(true);
    m_oSocket.setNoDelay(true);
    m_oSocket.connect(m_oAddr);
    AddEvent(EPOLLOUT);
    
	PAXOSLOG_ERROR << "start, ip " << GetSocketHost();
}
    
}


