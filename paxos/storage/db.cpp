/*
 * Module_name: db.cpp
 * Author: Barneyliu
 * Time: 2018-12-19
 * Description:
 *
 */

#include "db.h"
#include "commdef.h"

namespace multipaxos
{
////////////////////////

Database :: Database()
  : m_poValueStore(nullptr), m_sysvarStore(nullptr),m_masterStore(nullptr), m_minchosenStore(nullptr){
  m_bHasInit = false;
  m_iMyGroupIdx = -1;
}

Database :: ~Database(){
  delete m_poValueStore;
  delete m_sysvarStore;
  delete m_masterStore;
  delete m_minchosenStore;

  PAXOSLOG_INFO << "RocksDB Deleted. Path " << m_sDBPath;
}

int Database :: ClearAllLog(){
  string sSystemVariablesBuffer;
  int ret = GetSystemVariables(sSystemVariablesBuffer);
  if (ret != 0 && ret != 1){
    PAXOSLOG_ERROR << "GetSystemVariables fail, ret "<< ret;
    return ret;
  }

  string sMasterVariablesBuffer;
  ret = GetMasterVariables(sMasterVariablesBuffer);
  if (ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "GetMasterVariables fail, ret " << ret;
    return ret;
  }

  m_bHasInit = false;

  delete m_poValueStore;
  m_poValueStore = nullptr;

  delete m_sysvarStore;
  m_sysvarStore = nullptr;

  delete m_masterStore;
  m_masterStore = nullptr;

  delete m_minchosenStore;
  m_minchosenStore = nullptr;

  string sBakPath = m_sDBPath + ".bak";

  ret = FileUtils::DeleteDir(sBakPath);
  if (ret != 0) {
    PAXOSLOG_ERROR << "Delete bak dir fail, dir "<< sBakPath;
    return -1;
  }

  ret = rename(m_sDBPath.c_str(), sBakPath.c_str());
  assert(ret == 0);

  ret = Init(m_sDBPath, m_iMyGroupIdx);
  if (ret != 0) {
    PAXOSLOG_ERROR << "Init again fail, ret "<< ret;
    return ret;
  }

  WriteOptions oWriteOptions;
  oWriteOptions.bSync = true;
  if (sSystemVariablesBuffer.size() > 0) {
    ret = SetSystemVariables(oWriteOptions, sSystemVariablesBuffer);
    if (ret != 0) {
      PAXOSLOG_ERROR << "SetSystemVariables fail, ret " << ret;
      return ret;
    }
  }

  if (sMasterVariablesBuffer.size() > 0) {
    ret = SetMasterVariables(oWriteOptions, sMasterVariablesBuffer);
    if (ret != 0) {
      PAXOSLOG_ERROR << "SetMasterVariables fail, ret " << ret;
      return ret;
    }
  }

  return 0;
}

int Database :: Init(const std::string & sDBPath, const int iMyGroupIdx){
  if (m_bHasInit) {
    return 0;
  }

  m_iMyGroupIdx = iMyGroupIdx;
  m_sDBPath = sDBPath;

  m_poValueStore = new LogStore();
  assert(m_poValueStore != nullptr);
  int ret = m_poValueStore->Init(sDBPath, iMyGroupIdx);
  if (ret != 0) {
    PAXOSLOG_ERROR << "value store init fail, ret " << ret;
    return -1;
  }

  if(m_sysvarStore == nullptr) {
    m_sysvarStore = new ConfigStore();
    assert(m_sysvarStore != nullptr);
    ret = m_sysvarStore->Init(sDBPath, SystemVarType, 0);
    if(ret != 0) {
      PAXOSLOG_ERROR << "system variable store init fail, ret " << ret;
      return -1;
    }
  }

  if(m_masterStore == nullptr) {
    m_masterStore = new ConfigStore();
    assert(m_masterStore != nullptr);
    ret = m_masterStore->Init(sDBPath, MasterVarType, 0);
    if(ret != 0) {
      PAXOSLOG_ERROR << "master variable store init fail, ret " << ret;
      return -1;
    }
  }

  m_minchosenStore = new ConfigStore();
  assert(m_minchosenStore != nullptr);
  ret = m_minchosenStore->Init(sDBPath, MinChosenType, iMyGroupIdx);
  if(ret != 0) {
    PAXOSLOG_ERROR << "min chosen instance store init fail, ret " << ret;
    return -1;
  }

  m_bHasInit = true;
  PAXOSLOG_INFO << "OK, db_path " << sDBPath;
  return 0;
}

const std::string Database :: GetDBPath(){
  return m_sDBPath;
}

int Database :: GetMaxInstanceIDFileID(std::string & sFileID, uint64_t & llInstanceID){
  return m_poValueStore->GetMaxInstanceIDFileID(sFileID, llInstanceID);
}

  /*int Database :: RebuildOneIndex(const uint64_t& llInstanceID, const std::string & sFileID){
  return m_poValueStore->GetFileIDFromInstanceID(sFileID, llInstanceID);
  }*/

////////////////////////////////////////////////////////////////////////

int Database :: Get(const uint64_t llInstanceID, std::string & sValue){
  return m_poValueStore->Read(llInstanceID, sValue);
}

int Database :: ValueToFileID(const WriteOptions & oWriteOptions, const uint64_t llInstanceID,
			      const std::string & sValue, std::string & sFileID){
  int ret = m_poValueStore->Append(oWriteOptions, llInstanceID, sValue);
  if (ret != 0) {
    PAXOSLOG_ERROR << "fail, ret " << ret;
    return ret;
  }

  return 0;
}

int Database :: Put(const WriteOptions & oWriteOptions, const uint64_t llInstanceID, const std::string & sValue){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  std::string sFileID;
  return ValueToFileID(oWriteOptions, llInstanceID, sValue, sFileID);
}

int Database :: ForceDel(const WriteOptions & oWriteOptions, const uint64_t llInstanceID){
  return 0;
}

int Database :: Del(const WriteOptions & oWriteOptions, const uint64_t llInstanceID){
  return 0;
}

int Database :: GetMaxInstanceID(uint64_t & llInstanceID){
  std::string sFileID;
  return m_poValueStore->GetMaxInstanceIDFileID(sFileID, llInstanceID);
}

std::string Database :: GenKey(const uint64_t llInstanceID){
  string sKey;
  sKey.append((char *)&llInstanceID, sizeof(uint64_t));
  return sKey;
}

const uint64_t Database :: GetInstanceIDFromKey(const std::string & sKey){
  uint64_t llInstanceID = 0;
  memcpy(&llInstanceID, sKey.data(), sizeof(uint64_t));
  return llInstanceID;
}

int Database :: GetMinChosenInstanceID(uint64_t & llMinInstanceID){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_minchosenStore == nullptr) {
    PAXOSLOG_ERROR << "Min chosen instance handle is null, please check";
    return -1;
  }

  std::string sValue;
  int ret = m_minchosenStore->Get(sValue);
  if(ret != 0 && ret != 1) {
    PAXOSLOG_ERROR << "Get mix chosen instance failed, ret " << ret;
    return ret;
  }

  if(ret == 1 || sValue.empty()) {
    PAXOSLOG_ERROR << "no min chosen instanceid";
    llMinInstanceID = 0;
    return 0;
  }

  memcpy(&llMinInstanceID, sValue.c_str(), sValue.size());
  PAXOSLOG_ERROR << "ok, min chosen instanceid " << llMinInstanceID;
  return 0;
}

int Database :: SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const uint64_t llMinInstanceID){
  if(!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_minchosenStore == nullptr) {
    PAXOSLOG_ERROR << "Min chosen instance handle is null, please check";
    return -1;
  }

  if(!m_oTmpBuffer.Reserve(sizeof(uint64_t))) {
    PAXOSLOG_ERROR << "Apply tmp Append buffer memory len " << sizeof(uint64_t) << " failed";
    return -1;
  }
  
  memcpy((char *)m_oTmpBuffer.GetRawBuffer(), &llMinInstanceID, sizeof(uint64_t));

  int ret = m_minchosenStore->Set(oWriteOptions, m_oTmpBuffer.GetRawBuffer());
  if (ret != 0) {
    return ret;
  }

  PAXOSLOG_INFO << "ok, min chosen instanceid " << llMinInstanceID;
  return 0;
}


int Database :: SetSystemVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_sysvarStore == nullptr) {
    PAXOSLOG_ERROR << "system variables handle is null, please check";
    return -1;
  }

  return m_sysvarStore->Set(oWriteOptions, sBuffer);
}

int Database :: GetSystemVariables(std::string & sBuffer){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_sysvarStore == nullptr) {
    PAXOSLOG_ERROR << "system variables handle is null, please check";
    return -1;
  }

  int ret = m_sysvarStore->Get(sBuffer);
  if(ret)
    return ret;

  if(sBuffer.empty())
    return 1;
  return 0;
}

int Database :: SetMasterVariables(const WriteOptions & oWriteOptions, const std::string & sBuffer){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_masterStore == nullptr) {
    PAXOSLOG_ERROR << "master variables handle is null, please check";
    return -1;
  }

  return m_masterStore->Set(oWriteOptions, sBuffer);
}

int Database :: GetMasterVariables(std::string & sBuffer){
  if (!m_bHasInit) {
    PAXOSLOG_ERROR << "no init yet";
    return -1;
  }

  if(m_masterStore == nullptr) {
    PAXOSLOG_ERROR << "master variables handle is null, please check";
    return -1;
  }

  return m_masterStore->Get(sBuffer);
}

////////////////////////////////////////////////////

MultiDatabase :: MultiDatabase(){
}

MultiDatabase :: ~MultiDatabase(){
  for (auto & poDB : m_vecDBList) {
    delete poDB;
  }
}

int MultiDatabase :: Init(const std::string & sDBPath, const int iGroupCount){
  if (access(sDBPath.c_str(), F_OK) == -1) {
    PAXOSLOG_ERROR << "DBPath not exist or no limit to open, " << sDBPath;
    return -1;
  }

  if (iGroupCount < 1 || iGroupCount > 100000) {
    PAXOSLOG_ERROR << "Groupcount wrong " << iGroupCount;
    return -2;
  }

  std::string sNewDBPath = sDBPath;
  if (sDBPath[sDBPath.size() - 1] != '/') {
    sNewDBPath += '/';
  }

  for (int iGroupIdx = 0; iGroupIdx < iGroupCount; iGroupIdx++) {
    char sGroupDBPath[512] = {0};
    snprintf(sGroupDBPath, sizeof(sGroupDBPath), "%sg%d", sNewDBPath.c_str(), iGroupIdx);

    Database * poDB = new Database();
    assert(poDB != nullptr);
    m_vecDBList.push_back(poDB);

    if (poDB->Init(sGroupDBPath, iGroupIdx) != 0) {
      return -1;
    }
  }

  PAXOSLOG_INFO << "OK, DBPath " << sDBPath << " groupcount " << iGroupCount;
  return 0;
}

const std::string MultiDatabase :: GetLogStorageDirPath(const int iGroupIdx){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return "";
  }

  return m_vecDBList[iGroupIdx]->GetDBPath();
}

int MultiDatabase :: Get(const int iGroupIdx, const uint64_t llInstanceID, std::string & sValue){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->Get(llInstanceID, sValue);
}

int MultiDatabase :: Put(const WriteOptions & oWriteOptions, const int iGroupIdx,
			 const uint64_t llInstanceID, const std::string & sValue){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->Put(oWriteOptions, llInstanceID, sValue);
}

int MultiDatabase :: Del(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->Del(oWriteOptions, llInstanceID);
}

int MultiDatabase :: ForceDel(const WriteOptions & oWriteOptions, const int iGroupIdx, const uint64_t llInstanceID){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->ForceDel(oWriteOptions, llInstanceID);
}

int MultiDatabase :: GetMaxInstanceID(const int iGroupIdx, uint64_t & llInstanceID){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->GetMaxInstanceID(llInstanceID);
}
    
int MultiDatabase :: SetMinChosenInstanceID(const WriteOptions & oWriteOptions, const int iGroupIdx,
					    const uint64_t llMinInstanceID){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->SetMinChosenInstanceID(oWriteOptions, llMinInstanceID);
}

int MultiDatabase :: GetMinChosenInstanceID(const int iGroupIdx, uint64_t & llMinInstanceID){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->GetMinChosenInstanceID(llMinInstanceID);
}

int MultiDatabase :: ClearAllLog(const int iGroupIdx){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->ClearAllLog();
}

int MultiDatabase :: SetSystemVariables(const WriteOptions & oWriteOptions, const int iGroupIdx,
					const std::string & sBuffer){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->SetSystemVariables(oWriteOptions, sBuffer);
}

int MultiDatabase :: GetSystemVariables(const int iGroupIdx, std::string & sBuffer)
{
    if (iGroupIdx >= (int)m_vecDBList.size())
    {
        return -2;
    }

    return m_vecDBList[iGroupIdx]->GetSystemVariables(sBuffer);
}

int MultiDatabase :: SetMasterVariables(const WriteOptions & oWriteOptions, const int iGroupIdx,
					const std::string & sBuffer){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->SetMasterVariables(oWriteOptions, sBuffer);
}

int MultiDatabase :: GetMasterVariables(const int iGroupIdx, std::string & sBuffer){
  if (iGroupIdx >= (int)m_vecDBList.size()) {
    return -2;
  }

  return m_vecDBList[iGroupIdx]->GetMasterVariables(sBuffer);
}

}


