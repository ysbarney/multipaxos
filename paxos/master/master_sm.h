/*
 * Module_name: master_sm.h
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#pragma once

#include <mutex>
#include "sm.h"
#include "commdef.h"
#include "def.h"
#include "config_include.h"
#include "master_sm.pb.h"
#include "master_sm.h"
#include "master_variables_store.h"

namespace multipaxos 
{

enum MasterOperatorType{
			MasterOperatorType_Complete = 1,
};

class MasterStateMachine : public InsideSM 
{
public:
  MasterStateMachine(const LogStorage * poLogStorage, const nodeid_t iMyNodeID,
		     const int iGroupIdx, MasterChangeCallback pMasterChangeCallback);
  virtual ~MasterStateMachine();

  bool Execute(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue, SMCtx * poSMCtx);
  const int SMID() const {return MASTER_V_SMID;}

  bool ExecuteForCheckpoint(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sPaxosValue){
    return true;
  }

  const uint64_t GetCheckpointInstanceID(const int iGroupIdx) const {
    return m_llMasterVersion;
  }

  void BeforePropose(const int iGroupIdx, std::string & sValue);
  const bool NeedCallBeforePropose();

public:
  int GetCheckpointState(const int iGroupIdx, std::string & sDirPath, std::vector<std::string> & vecFileList) {
    return 0;
  }    

  int LoadCheckpointState(const int iGroupIdx, const std::string & sCheckpointTmpFileDirPath,
			  const std::vector<std::string> & vecFileList, const uint64_t llCheckpointInstanceID) {
    return 0;
  }

  int LockCheckpointState() {
    return 0;
  }

  void UnLockCheckpointState() {
  }

public:
  int Init();

  int LearnMaster(const uint64_t llInstanceID, const MasterOperator & oMasterOper,
		  const uint64_t llAbsMasterTimeout = 0);

  const nodeid_t GetMaster(const int iGroupIdx);
  const nodeid_t GetMasterWithVersion(uint64_t & llVersion);
  const bool IsIMMaster(const int iGroupIdx);
  void UpdateMasterStat(const std::string& sMasterStat);
  int BuildMasterStatMap(const std::string& sMasterStat);
  int InitMasterStatMap();

  const std::map<int, nodeid_t>& GetMasterStat() const {
    return m_MasterStatMap;
  }

  void SetMasterStat(const std::map<int, nodeid_t>& MasterStatMap) {
    m_MasterStatMap = MasterStatMap;
  }
  
public:
  int UpdateMasterToStore(const nodeid_t llMasterNodeID, const uint64_t llVersion, const uint32_t iLeaseTime);
  void SafeGetMaster(nodeid_t & iMasterNodeID, uint64_t & llMasterVersion);

public:
    static bool MakeOpValue(const nodeid_t iNodeID, const std::string& sMasterStat, const uint64_t llVersion,
			    const int iTimeout, const MasterOperatorType iOp, std::string & sPaxosValue);

public:
  int GetCheckpointBuffer(std::string & sCPBuffer);
  int UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange);

private:
  int m_iMyGroupIdx;
  nodeid_t m_iMyNodeID;

private:
  MasterVariablesStore m_oMVStore;

  nodeid_t m_iMasterNodeID;
  uint64_t m_llMasterVersion;
  int m_iLeaseTime;
  uint64_t m_llAbsExpireTime;

  std::mutex m_oMutex;

  MasterChangeCallback m_pMasterChangeCallback;

  std::map<int, nodeid_t> m_MasterStatMap;
};
    
}

