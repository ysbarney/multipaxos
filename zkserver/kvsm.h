#pragma once

#include "sm.h"
#include "kv.h"
#include <limits>
#include "def.h"
#include "async_boost_log.h"

namespace phxkv
{

class PhxKVSMCtx
{
public:
  int iExecuteRet;
  std::string sReadValue;
  uint64_t llReadVersion;

  PhxKVSMCtx() {
    iExecuteRet = -1;
    llReadVersion = 0;
  }
};

////////////////////////////////////////////////

class PhxKVSM : public multipaxos::StateMachine
{
public:
  PhxKVSM(const std::string & sDBPath);
  virtual ~PhxKVSM();

  const bool Init(multipaxos::Node* poPaxosNode);
  bool Execute(const int iGroupIdx, const uint64_t llInstanceID, const std::string &sPaxosValue,
	       multipaxos::SMCtx * poSMCtx);

  const int SMID() const {return 1;}

public:
    //no use
  bool ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sPaxosValue) {
    return true;
  }

  //have checkpoint.
  const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const { return m_llCheckpointInstanceID;}

public:
  //have checkpoint, but not impl auto copy checkpoint to other node, so return fail.
  int LockCheckpointState() { return -1; }
  int GetCheckpointState(const int iGroupIdx, std::string & sDirPath, std::vector<std::string> & vecFileList) {
    return -1;
  }

  void UnLockCheckpointState() { }
  int LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
			  const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID) {
    return -1;
  }

public:
  static bool MakeOpValue(const std::string & sKey, const std::string & sValue, const uint32_t node_type,
			  const uint32_t lxid, const uint64_t llVersion, const KVOperatorType iOp,
			  std::string & sPaxosValue);

  static bool MakeGetOpValue(const std::string & sKey, std::string & sPaxosValue);
  static bool MakeSetOpValue(const std::string & sKey, const std::string & sValue, const uint32_t lxid,
			     const uint64_t llVersion, std::string & sPaxosValue);

  static bool MakeCreateOpValue(const std::string & sKey, const std::string & sValue, const uint32_t node_type,
				const uint32_t lxid, const uint64_t llVersion, std::string & sPaxosValue);

  static bool MakeDelOpValue(const std::string & sKey, const uint32_t lxid, const uint64_t llVersion,
			     std::string & sPaxosValue);

  KVClient * GetKVClient();
  int SyncCheckpointInstanceID(const int iGroupIdx, const uint64_t llInstanceID);

private:
  std::string m_sDBPath;
  KVClient m_oKVClient;

  multipaxos::Node* m_poPaxosNode;
  uint64_t m_llCheckpointInstanceID;
  int m_iSkipSyncCheckpointTimes;
};
    
}

