/*
 * Module_name: ioloop.h
 * Author: Barneyliu
 * Time: 2018-12-28
 * Description:
 *
 */

#pragma once

#include <map>
#include <string>
#include "comm_include.h"
#include "config_include.h"
#include <queue>

namespace multipaxos
{

#define RETRY_QUEUE_MAX_LEN 300

class Instance;

class IOLoop : public Thread
{
public:
  IOLoop(Config * poConfig, Instance * poInstance);
  virtual ~IOLoop();

  void run();
  void Stop();
  void OneLoop(const int iTimeoutMs);
  void DealWithRetry();
  void ClearRetryQueue();

public:
  int AddMessage(const char * pcMessage, const int iMessageLen);
  int AddRetryPaxosMsg(const PaxosMsg & oPaxosMsg);

public:
  virtual bool AddTimer(const int iTimeout, const int iType, const uint64_t llInstanceID,
			uint32_t & iTimerID);

  virtual void RemoveTimer(uint32_t iTimerID);
  void DealwithTimeout(int & iNextTimeout);
  void DealwithTimeoutOne(const uint32_t iTimerID, const int iType, const uint64_t llInstanceID);

private:
  bool m_bIsEnd;
  bool m_bIsStart;
  Timer m_oTimer;
  std::map<uint32_t, bool> m_mapTimerIDExist;

  concurrent_queue<std::string *> m_oMessageQueue;
  concurrent_queue<PaxosMsg> m_oRetryQueue;

  int m_iQueueMemSize;

  Config * m_poConfig;
  Instance * m_poInstance;
};
    
}

