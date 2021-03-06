/*
 * Module_name: paxos_log.h
 * Author: Barneyliu
 * Time: 2018-12-21
 * Description:
 *
 */

#pragma once

#include <string>
#include <vector>
#include <inttypes.h>
#include "storage.h"
#include "paxos_msg.pb.h"

namespace multipaxos
{

class PaxosLog
{
public:
  PaxosLog(const LogStorage * poLogStorage);
  virtual ~PaxosLog();

  int WriteLog(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID,
	       const std::string & sValue);

  int ReadLog(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue);
  int GetMaxInstanceIDFromLog(const int iGroupIdx, uint64_t & llInstanceID);

  int WriteState(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID,
		 const AcceptorStateData & oState);
  int ReadState(const int iGroupIdx, const uint64_t llInstanceID, AcceptorStateData & oState);

private:
  LogStorage * m_poLogStorage;
};

}

