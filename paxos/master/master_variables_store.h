/*
 * Module_name: master_variables_store.h
 * Author: Barneyliu
 * Time: 2019-01-10
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

class MasterVariablesStore
{
public:
  MasterVariablesStore(const LogStorage * poLogStorage);
  virtual ~MasterVariablesStore();

  int Write(const WriteOptions & oWriteOptions,  const int iGroupIdx, const MasterVariables & oVariables);
  int Write(const WriteOptions &oWriteOptions, const int iGroupIdx, const std::string& sValue);
  int Read(const int iGroupIdx, MasterVariables & oVariables);
  int Read(const int iGroupIdx, std::string& sValue);
  bool Match_checksum(const std::string& sValue);

private:
  LogStorage * m_poLogStorage;
  uint32_t m_ichksum;
};

}

