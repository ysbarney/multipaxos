/*
 * Module_name: config_store.h
 * Author: Barneyliu
 * Time: 2019-04-22
 * Description:
 *
 */

#pragma once
#include <string>
#include <mutex>
#include "buffer.hpp"
#include "util.h"
#include "storage.h"
#include "inside_options.h"
#include <map>

namespace multipaxos
{

enum ConfigType {
		 SystemVarType,
		 MasterVarType,
		 MinChosenType
};

class ConfigStore {
public:
  ConfigStore();
  virtual ~ConfigStore();

  int Init(const std::string & sPath, enum ConfigType type, const int iMyGroupIdx);
  int Get(std::string& sValue);
  int Set(const WriteOptions& oWriteOptions, const std::string& sValue);
  
private:
  int m_iFd;
  std::string m_filename;
  int m_iMyGroupIdx;
  std::string m_value;
  uint32_t m_checksum;
  ardb::Buffer m_oTmpBuffer;
  bool b_init;
};
}
