/*
 * Module_name: ioloop.cpp
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#include "ioloop.h"
#include "instance.h"

using namespace std;

namespace multipaxos
{
  IOLoop :: IOLoop(Config * poConfig, Instance * poInstance)
    : m_poConfig(poConfig), m_poInstance(poInstance){
    m_bIsEnd = false;
    m_bIsStart = false;

    m_iQueueMemSize = 0;
}

IOLoop :: ~IOLoop(){
}

void IOLoop :: run(){
  m_bIsEnd = false;
  m_bIsStart = true;
  while(true) {
    int iNextTimeout = 1000;

    DealwithTimeout(iNextTimeout);

    OneLoop(iNextTimeout);

    if (m_bIsEnd) {
      PAXOSLOG_DEBUG << "IOLoop [End]";
      break;
    }
  }
}


int IOLoop :: AddMessage(const char * pcMessage, const int iMessageLen){
  while(true) {
    if ((int)m_oMessageQueue.size() > QUEUE_MAXLENGTH)  {
      PAXOSLOG_ERROR << "Queue full, skip msg";
      usleep(1000 * 20);
      continue;
    }

    if (m_iQueueMemSize > MAX_QUEUE_MEM_SIZE) {
      PAXOSLOG_ERROR << "queue memsize " << m_iQueueMemSize << " too large, can't enqueue";
      usleep(1000 * 20);
      continue;
    }
    break;
  }

  m_oMessageQueue.add(new string(pcMessage, iMessageLen));

  m_iQueueMemSize += iMessageLen;

  return 0;
}

int IOLoop :: AddRetryPaxosMsg(const PaxosMsg & oPaxosMsg){
  if (m_oRetryQueue.size() > RETRY_QUEUE_MAX_LEN)  {
    m_oRetryQueue.pop();
  }

  m_oRetryQueue.add(oPaxosMsg);
  return 0;
}

void IOLoop :: Stop(){
  m_bIsEnd = true;
  if (m_bIsStart) {
    join();
  }
}

void IOLoop :: ClearRetryQueue(){
  while (!m_oRetryQueue.empty()) {
    m_oRetryQueue.pop();
  }
}

void IOLoop :: DealWithRetry() {
  if (m_oRetryQueue.empty()){
    return;
  }

  bool bHaveRetryOne = false;
  while (!m_oRetryQueue.empty()) {
    PaxosMsg oPaxosMsg; 
    if(!m_oRetryQueue.peek(oPaxosMsg))
      continue;

    if (oPaxosMsg.instanceid() > m_poInstance->GetNowInstanceID() + 1) {
      break;
    } else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID() + 1) {
      //only after retry i == now_i, than we can retry i + 1.
      if (bHaveRetryOne) {
	PAXOSLOG_DEBUG << "retry msg (i+1). instanceid " << oPaxosMsg.instanceid();
	m_poInstance->OnReceivePaxosMsg(oPaxosMsg, true);
      } else {
	break;
      }
    } else if (oPaxosMsg.instanceid() == m_poInstance->GetNowInstanceID()){
      PAXOSLOG_DEBUG << "retry msg. instanceid " << oPaxosMsg.instanceid();
      m_poInstance->OnReceivePaxosMsg(oPaxosMsg);
      bHaveRetryOne = true;
    }
  }
}

void IOLoop :: OneLoop(const int iTimeoutMs){
  std::string * psMessage = nullptr;

  bool bSucc = m_oMessageQueue.peek(psMessage, iTimeoutMs);
  if(bSucc) {
    if (psMessage != nullptr && psMessage->size() > 0){
      m_iQueueMemSize -= psMessage->size();
      m_poInstance->OnReceive(*psMessage);
    }

    delete psMessage;
  }

  DealWithRetry();
}

bool IOLoop :: AddTimer(const int iTimeout, const int iType, const uint64_t llInstanceID,
			uint32_t & iTimerID){
  if (iTimeout == -1){
    return true;
  }

  uint64_t llAbsTime = Time::GetSteadyClockMS() + iTimeout;
  m_oTimer.AddTimerWithType(llAbsTime, iType, llInstanceID, iTimerID);

  m_mapTimerIDExist[iTimerID] = true;

  return true;
}

void IOLoop :: RemoveTimer(uint32_t iTimerID){
  auto it = m_mapTimerIDExist.find(iTimerID);
  if (it != end(m_mapTimerIDExist)){
    m_mapTimerIDExist.erase(it);
  }

  iTimerID = 0;
}

void IOLoop :: DealwithTimeoutOne(const uint32_t iTimerID, const int iType, const uint64_t llInstanceID){
  auto it = m_mapTimerIDExist.find(iTimerID);
  if (it == end(m_mapTimerIDExist)) {
    PAXOSLOG_ERROR << "Timeout aready remove!, timerid " << iTimerID << " iType " << iType;
    return;
  }

  m_mapTimerIDExist.erase(it);

  m_poInstance->OnTimeout(iTimerID, iType, llInstanceID);
}

void IOLoop :: DealwithTimeout(int & iNextTimeout){
  bool bHasTimeout = true;

  while(bHasTimeout) {
    uint32_t iTimerID = 0;
    int iType = 0;
    uint64_t llInstanceID = 0;
    bHasTimeout = m_oTimer.PopTimeout(iTimerID, iType, llInstanceID);

    if (bHasTimeout) {
      DealwithTimeoutOne(iTimerID, iType, llInstanceID);

      iNextTimeout = m_oTimer.GetNextTimeout();
      if (iNextTimeout != 0) {
	break;
      }
    }
  }
}

}


