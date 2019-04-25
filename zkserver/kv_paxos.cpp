#include "kv_paxos.h"
#include "zk_server.h"
#include <assert.h>
#include <string>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace multipaxos;
using namespace std;

namespace phxkv
{

PhxKV :: PhxKV(const multipaxos::NodeInfo & oMyNode, const multipaxos::NodeInfoList & vecNodeList,
	       const multipaxos::NodeInfo & oExtraMyNode, const multipaxos::NodeInfoList & vecExtraNodeList,
	       const std::string & sKVDBPath, const std::string & sPaxosLogPath)
  : m_oMyNode(oMyNode), m_vecNodeList(vecNodeList), m_oExtraMyNode(oExtraMyNode),
    m_vecExtraNodeList(vecExtraNodeList), m_sKVDBPath(sKVDBPath), m_sPaxosLogPath(sPaxosLogPath),
    m_poPaxosNode(nullptr), m_oPhxKVSM(sKVDBPath){
  //only show you how to use multi paxos group, you can set as 1, 2, or any other number.
  //not too large.
  m_iGroupCount = 1;
}

PhxKV :: ~PhxKV(){
  delete m_poPaxosNode;
}

const multipaxos::NodeInfo PhxKV :: GetMaster(const std::string & sKey){
  int iGroupIdx = GetGroupIdx(sKey);
  return m_poPaxosNode->GetMaster(iGroupIdx);
}

const bool PhxKV :: IsIMMaster(const std::string & sKey){
  int iGroupIdx = GetGroupIdx(sKey);
  return m_poPaxosNode->IsIMMaster(iGroupIdx);
}

int PhxKV :: RunPaxos(){
  Options oOptions;
  oOptions.sLogStoragePath = m_sPaxosLogPath;

  //this groupcount means run paxos group count.
  //every paxos group is independent, there are no any communicate between any 2 paxos group.
  oOptions.iGroupCount = m_iGroupCount;

  oOptions.oMyNode = m_oMyNode;
  oOptions.vecNodeInfoList = m_vecNodeList;
  oOptions.oMyExtraNode = m_oExtraMyNode;
  oOptions.vecExtraNodeInfoList = m_vecExtraNodeList;
  oOptions.bUseCheckpointReplayer = true;

  //because all group share state machine(kv), so every group have same state machine.
  //just for split key to different paxos group, to upgrate performance.
  for (int iGroupIdx = 0; iGroupIdx < m_iGroupCount; iGroupIdx++) {
    GroupSMInfo oSMInfo;
    oSMInfo.iGroupIdx = iGroupIdx;
    oSMInfo.vecSMList.push_back(&m_oPhxKVSM);
    oSMInfo.bIsUseMaster = true;

    oOptions.vecGroupSMInfoList.push_back(oSMInfo);
  }

  int ret = Node::RunNode(oOptions, m_poPaxosNode);
  if (ret != 0) {
    ZKLOG_ERROR << "run paxos fail, ret " << ret;
    return ret;
  }

  bool bSucc = m_oPhxKVSM.Init(m_poPaxosNode);
  if (!bSucc) {
    return -1;
  }

  ZKLOG_INFO << "run paxos ok";
  return 0;
}

int PhxKV :: GetGroupIdx(const std::string & sKey){
  uint32_t iHashNum = 0;
  for (size_t i = 0; i < sKey.size(); i++) {
    iHashNum = iHashNum * 7 + ((int)sKey[i]);
  }

  return iHashNum % m_iGroupCount;
}

int PhxKV :: KVPropose(const std::string & sKey, const std::string & sPaxosValue, PhxKVSMCtx & oPhxKVSMCtx){
  int iGroupIdx = GetGroupIdx(sKey);

  SMCtx oCtx;
  //smid must same to PhxKVSM.SMID().
  oCtx.m_iSMID = 1;
  oCtx.m_pCtx = (void *)&oPhxKVSMCtx;

  uint64_t llInstanceID = 0;
  int ret = m_poPaxosNode->Propose(iGroupIdx, sPaxosValue, llInstanceID, &oCtx);
  if (ret != 0)	{
    ZKLOG_ERROR << "paxos propose fail, key " << sKey << " groupidx " << iGroupIdx << " ret " << ret;
    return ret;
  }
  return 0;
}

PhxKVStatus PhxKV :: Set(const std::string & sKey, const std::string & sValue, const uint32_t lxid,
			 const uint64_t llVersion){
  string sPaxosValue;
  bool bSucc = PhxKVSM::MakeSetOpValue(sKey, sValue, lxid, llVersion, sPaxosValue);
  if (!bSucc) {
    return PhxKVStatus::FAIL;
  }

  PhxKVSMCtx oPhxKVSMCtx;
  int ret = KVPropose(sKey, sPaxosValue, oPhxKVSMCtx);
  if (ret != 0) {
    return PhxKVStatus::FAIL;
  }

  if (oPhxKVSMCtx.iExecuteRet == ZOK) {
    return PhxKVStatus::SUCC;
  } else if (oPhxKVSMCtx.iExecuteRet == ZBADVERSION) {
    return PhxKVStatus::VERSION_CONFLICT;
  } else {
    return PhxKVStatus::FAIL;
  }
}

PhxKVStatus PhxKV :: Create(const std::string & sKey, const std::string & sValue, const uint32_t node_type,
			    const uint32_t lxid, const uint64_t llVersion){
  string sPaxosValue;
  bool bSucc = PhxKVSM::MakeCreateOpValue(sKey, sValue, node_type, lxid, llVersion, sPaxosValue);
  if (!bSucc) {
    return PhxKVStatus::FAIL;
  }

  PhxKVSMCtx oPhxKVSMCtx;
  int ret = KVPropose(sKey, sPaxosValue, oPhxKVSMCtx);
  if (ret != 0)	{
    return PhxKVStatus::FAIL;
  }

  if (oPhxKVSMCtx.iExecuteRet == ZOK){
    return PhxKVStatus::SUCC;
  } else if (oPhxKVSMCtx.iExecuteRet == ZBADVERSION){
    return PhxKVStatus::VERSION_CONFLICT;
  } else {
    return PhxKVStatus::FAIL;
  }
}

PhxKVStatus PhxKV :: GetLocal(const std::string & sKey, std::string & sValue, uint64_t & llVersion){
  int ret = m_oPhxKVSM.GetKVClient()->Get(sKey, sValue, llVersion);
  if (ret == ZOK) {
    return PhxKVStatus::SUCC;
  } else {
    ret = ZkReply::get_instance()->Get_ephemeral_value(sKey, sValue, llVersion);
    if(ret == ZOK)
      return  PhxKVStatus::SUCC;
    else if(ret == ZNONODE)
      return PhxKVStatus::KEY_NOTEXIST;
    else
      return PhxKVStatus::FAIL;
  }
}

PhxKVStatus PhxKV :: GetChild(const std::string & sKey, std::vector<std::string>& sValue, uint64_t & llVersion){
  ZkReply::get_instance()->GetChild_ephemeral_value(sKey, sValue);
  int ret = m_oPhxKVSM.GetKVClient()->GetChild(sKey, sValue, llVersion);
  if (ret == ZOK) {
    return PhxKVStatus::SUCC;
  } else {
    return PhxKVStatus::FAIL;
  }
}

PhxKVStatus PhxKV :: Delete(const std::string & sKey, const uint32_t lxid,
			    const uint64_t llVersion){
  string sPaxosValue;
  bool bSucc = PhxKVSM::MakeDelOpValue(sKey, lxid, llVersion, sPaxosValue);
  if (!bSucc) {
    return PhxKVStatus::FAIL;
  }

  PhxKVSMCtx oPhxKVSMCtx;
  int ret = KVPropose(sKey, sPaxosValue, oPhxKVSMCtx);
  if (ret != 0) {
    return PhxKVStatus::FAIL;
  }

  if (oPhxKVSMCtx.iExecuteRet == ZOK) {
    return PhxKVStatus::SUCC;
  } else if (oPhxKVSMCtx.iExecuteRet == ZBADVERSION) {
    return PhxKVStatus::VERSION_CONFLICT;
  } else {
    return PhxKVStatus::FAIL;
  }
}

}

