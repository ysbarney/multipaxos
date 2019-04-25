#pragma once

#include "node.h"
#include "kvsm.h"
#include <string>
#include <vector>
#include "def.h"
#include "async_boost_log.h"

namespace phxkv
{

class PhxKV
{
public:
  PhxKV(const multipaxos::NodeInfo & oMyNode, const multipaxos::NodeInfoList & vecNodeList,
	const multipaxos::NodeInfo & oExtraMyNode, const multipaxos::NodeInfoList & vecExtraNodeList,
	const std::string & sKVDBPath, const std::string & sPaxosLogPath);
  virtual ~PhxKV();
  int RunPaxos();

  const multipaxos::NodeInfo GetMaster(const std::string & sKey);
  const bool IsIMMaster(const std::string & sKey);

  PhxKVStatus Set(const std::string & sKey, const std::string & sValue,	const uint32_t lxid,
		  const uint64_t llVersion = NullVersion);

  PhxKVStatus Create(const std::string & sKey, const std::string & sValue, const uint32_t node_type,
		     const uint32_t lxid, const uint64_t llVersion = NullVersion);

  PhxKVStatus GetLocal(const std::string & sKey, std::string & sValue, uint64_t & llVersion);
  PhxKVStatus GetChild(const std::string & sKey, std::vector<std::string>& sValue, uint64_t & llVersion);
  PhxKVStatus Delete(const std::string & sKey, const uint32_t lxid, const uint64_t llVersion = NullVersion);

private:
  int GetGroupIdx(const std::string & sKey);
  int KVPropose(const std::string & sKey, const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx);

private:
  multipaxos::NodeInfo m_oMyNode;
  multipaxos::NodeInfoList m_vecNodeList;
  multipaxos::NodeInfo m_oExtraMyNode;
  multipaxos::NodeInfoList m_vecExtraNodeList;
  std::string m_sKVDBPath;
  std::string m_sPaxosLogPath;

  int m_iGroupCount;
  multipaxos::Node * m_poPaxosNode;
  PhxKVSM m_oPhxKVSM;
};
    
}


