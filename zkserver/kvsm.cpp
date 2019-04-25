#include "zk_server.h"
#include "kvsm.h"
#include "phxkv.pb.h"
#include <algorithm>

using namespace std;

namespace phxkv
{

PhxKVSM :: PhxKVSM(const std::string & sDBPath)
  : m_llCheckpointInstanceID(multipaxos::NoCheckpoint), m_iSkipSyncCheckpointTimes(0){
  m_sDBPath = sDBPath;
}

PhxKVSM :: ~PhxKVSM(){
}

const bool PhxKVSM :: Init(multipaxos::Node* poPaxosNode){
  bool bSucc = m_oKVClient.Init(m_sDBPath);
  if (!bSucc) {
    ZKLOG_ERROR << "KVClient.Init fail, dbpath " << m_sDBPath;
    return false;
  }

  m_poPaxosNode = poPaxosNode;
  /*int ret = m_oKVClient.GetCheckpointInstanceID(m_llCheckpointInstanceID);
  if (ret != 0 && ret != ZNONODE) {
    ZKLOG_ERROR << "KVClient.GetCheckpointInstanceID fail, ret " << ret;
    return false;
  }

  if (ret == ZNONODE) {
    ZKLOG_INFO << "no checkpoint";
    m_llCheckpointInstanceID = multipaxos::NoCheckpoint;
  } else {
    ZKLOG_INFO << "CheckpointInstanceID " << m_llCheckpointInstanceID;
    }*/

  return true;
}

int PhxKVSM :: SyncCheckpointInstanceID(const int iGroupIdx, const uint64_t llInstanceID){
  if (m_iSkipSyncCheckpointTimes++ < 50) {
    ZKLOG_DEBUG << "no need to sync checkpoint, skiptimes " << m_iSkipSyncCheckpointTimes;
    return 0;
  }

  //int ret = m_oKVClient.SetCheckpointInstanceID(llInstanceID);
  int ret = m_poPaxosNode->SetMinChosenInstanceID(iGroupIdx, llInstanceID);
  if (ret != 0) {
    ZKLOG_ERROR << "Node SetMinChosenInstanceID fail, ret " << ret << " instanceid " << llInstanceID;
    return ret;
  }

  ZKLOG_INFO << "ok, old checkpoint instanceid " << m_llCheckpointInstanceID << " new checkpoint instanceid "
	     << llInstanceID;

  m_llCheckpointInstanceID = llInstanceID;
  m_iSkipSyncCheckpointTimes = 0;
  return 0;
}

bool PhxKVSM :: Execute(const int iGroupIdx, const uint64_t llInstanceID,
			const std::string & sPaxosValue, multipaxos::SMCtx * poSMCtx){
  KVOperator oKVOper;
  bool bSucc = oKVOper.ParseFromArray(sPaxosValue.data(), sPaxosValue.size());
  if (!bSucc) {
    ZKLOG_ERROR << "oKVOper data wrong";
    //wrong oper data, just skip, so return true
    return true;
  }

  int iExecuteRet = -1;
  string sReadValue;
  uint64_t llReadVersion;

  if (oKVOper.operator_() == KVOperatorType_READ) {
      iExecuteRet = m_oKVClient.Get(oKVOper.key(), sReadValue, llReadVersion);
      if(iExecuteRet == ZNONODE)
	iExecuteRet = ZkReply::get_instance()->Get_ephemeral_value(oKVOper.key(), sReadValue, llReadVersion);
  } else if (oKVOper.operator_() == KVOperatorType_SET) {     
    iExecuteRet = m_oKVClient.Set(oKVOper.key(), oKVOper.value(), oKVOper.version());
    if(iExecuteRet == ZNONODE)
      iExecuteRet = ZkReply::get_instance()->Set_ephemeral_value(oKVOper.key(), oKVOper.value(), oKVOper.version());
    
    ZkReply::get_instance()->request_reply(oKVOper.key(), oKVOper.sid(), KVOperatorType_SET, iExecuteRet);
    if(!iExecuteRet) {
      ZkReply::get_instance()->watch_data_reply(oKVOper.key(), CHANGED_EVENT_DEF);
      std::string key = oKVOper.key();
      std::string parentKey = key.substr(0, key.rfind("/"));
      if(!parentKey.empty()) {
	ZkReply::get_instance()->watch_data_reply(parentKey, CHILD_EVENT_DEF);
      }
    }
  } else if (oKVOper.operator_() == KVOperatorType_DELETE) {
    iExecuteRet = m_oKVClient.Del(oKVOper.key(), oKVOper.version());
    if(iExecuteRet == ZNONODE) {
      iExecuteRet = ZkReply::get_instance()->Del_ephemeral_value(oKVOper.key(), oKVOper.version());
      if(iExecuteRet == ZOK)
	ZkReply::get_instance()->Remove_ephemeral_value(oKVOper.key());
    }
    
    ZkReply::get_instance()->request_reply(oKVOper.key(), oKVOper.sid(), KVOperatorType_DELETE, iExecuteRet);
    if(!iExecuteRet) {
      ZkReply::get_instance()->watch_data_reply(oKVOper.key(), DELETED_EVENT_DEF);
      std::string key = oKVOper.key();
      std::string parentKey = key.substr(0, key.rfind("/"));
      if(!parentKey.empty()) {
	ZkReply::get_instance()->watch_data_reply(parentKey, CHILD_EVENT_DEF);
      }
    }
  } else if(oKVOper.operator_() == KVOperatorType_CREATE) {
    if (oKVOper.nodetype() == 1)
      iExecuteRet = ZkReply::get_instance()->Create_ephemeral_value(oKVOper.key(), oKVOper.value(),
								    oKVOper.version());
    else
      iExecuteRet = m_oKVClient.Create(oKVOper.key(), oKVOper.value(), oKVOper.version());

    ZkReply::get_instance()->request_reply(oKVOper.key(), oKVOper.sid(), KVOperatorType_CREATE, iExecuteRet);
    if(!iExecuteRet) {
      ZkReply::get_instance()->watch_data_reply(oKVOper.key(), CREATED_EVENT_DEF);
      std::string key = oKVOper.key();
      std::string parentKey = key.substr(0, key.rfind("/"));
      if(!parentKey.empty()) {
	ZkReply::get_instance()->watch_data_reply(parentKey, CHILD_EVENT_DEF);
      }
    }
  } else {
    ZKLOG_ERROR << "unknown op " << oKVOper.operator_();
    //wrong op, just skip, so return true;
    return true;
  }

  if (iExecuteRet == ZSYSTEMERROR) {
    //need retry
    //return false;
    return true;
  } else {
    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr) {
      PhxKVSMCtx * poPhxKVSMCtx = (PhxKVSMCtx *)poSMCtx->m_pCtx;
      poPhxKVSMCtx->iExecuteRet = iExecuteRet;
      poPhxKVSMCtx->sReadValue = sReadValue;
      poPhxKVSMCtx->llReadVersion = llReadVersion;
    }

    SyncCheckpointInstanceID(iGroupIdx, llInstanceID);
    return true;
  }
}

////////////////////////////////////////////////////

bool PhxKVSM :: MakeOpValue(const std::string & sKey, const std::string & sValue, const uint32_t node_type,
			    const uint32_t lxid, const uint64_t llVersion, const KVOperatorType iOp,
			    std::string & sPaxosValue){
  KVOperator oKVOper;
  oKVOper.set_key(sKey);
  oKVOper.set_value(sValue);
  oKVOper.set_version(llVersion);
  oKVOper.set_operator_(iOp);
  oKVOper.set_nodetype(node_type);
  oKVOper.set_sid(lxid);

  return oKVOper.SerializeToString(&sPaxosValue);
}

bool PhxKVSM :: MakeGetOpValue(const std::string & sKey, std::string & sPaxosValue){
  return MakeOpValue(sKey, "", 0, 0, 0, KVOperatorType_READ, sPaxosValue);
}

bool PhxKVSM :: MakeSetOpValue(const std::string & sKey, const std::string & sValue, const uint32_t lxid,
			       const uint64_t llVersion, std::string & sPaxosValue){
  return MakeOpValue(sKey, sValue, 0, lxid, llVersion, KVOperatorType_SET, sPaxosValue);
}

bool PhxKVSM :: MakeCreateOpValue(const std::string & sKey, const std::string& sValue, const uint32_t node_type,
				  const uint32_t lxid, const uint64_t llVersion, std::string & sPaxosValue){
  return MakeOpValue(sKey, sValue, node_type, lxid, llVersion, KVOperatorType_CREATE, sPaxosValue);
}


bool PhxKVSM :: MakeDelOpValue(const std::string & sKey, const uint32_t lxid, const uint64_t llVersion,
			       std::string & sPaxosValue){
  return MakeOpValue(sKey, "", 0, lxid, llVersion, KVOperatorType_DELETE, sPaxosValue);
}

KVClient * PhxKVSM :: GetKVClient(){
  return &m_oKVClient;
}

}

