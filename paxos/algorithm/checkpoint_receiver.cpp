/*
 * Module_name: checkpoint_receiver.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "checkpoint_receiver.h"
#include "comm_include.h"
#include <vector>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using namespace std;

namespace multipaxos
{

CheckpointReceiver :: CheckpointReceiver(Config * poConfig, LogStorage * poLogStorage)
  : m_poConfig(poConfig), m_poLogStorage(poLogStorage) {
  Reset();
}

CheckpointReceiver :: ~CheckpointReceiver() {
}

void CheckpointReceiver :: Reset() {
  m_mapHasInitDir.clear();
  m_iSenderNodeID = nullnode;
  m_llUUID = 0;
  m_llSequence = 0;
}

int CheckpointReceiver :: NewReceiver(const nodeid_t iSenderNodeID, const uint64_t llUUID) {
  int ret = ClearCheckpointTmp();
  if (ret != 0) {
    return ret;
  }

  ret = m_poLogStorage->ClearAllLog(m_poConfig->GetMyGroupIdx());
  if (ret != 0) {
    PAXOSLOG_ERROR << "ClearAllLog fail, groupidx " << m_poConfig->GetMyGroupIdx()
		   << " ret " << ret;
    return ret;
  }
    
  m_mapHasInitDir.clear();

  m_iSenderNodeID = iSenderNodeID;
  m_llUUID = llUUID;
  m_llSequence = 0;

  return 0;
}

int CheckpointReceiver :: ClearCheckpointTmp() {
  string sLogStoragePath = m_poLogStorage->GetLogStorageDirPath(m_poConfig->GetMyGroupIdx());

  DIR * dir = nullptr;
  struct dirent  * ptr;

  dir = opendir(sLogStoragePath.c_str());
  if (dir == nullptr) {
    return -1;
  }

  int ret = 0;
  while ((ptr = readdir(dir)) != nullptr) {
    if (string(ptr->d_name).find("cp_tmp_") != std::string::npos) {
      char sChildPath[1024] = {0};
      snprintf(sChildPath, sizeof(sChildPath), "%s/%s", sLogStoragePath.c_str(), ptr->d_name);
      ret = FileUtils::DeleteDir(sChildPath);
      if (ret != 0){
	break;
      }   
    }
  }

  closedir(dir);
  return ret;
}

const bool CheckpointReceiver::IsReceiverFinish(const nodeid_t iSenderNodeID,
	    const uint64_t llUUID, const uint64_t llEndSequence) {
  if (iSenderNodeID == m_iSenderNodeID && llUUID == m_llUUID
      && llEndSequence == m_llSequence + 1) {
    return true;
  } else {
    return false;
  }
}

const std::string CheckpointReceiver :: GetTmpDirPath(const int iSMID) {
  string sLogStoragePath = m_poLogStorage->GetLogStorageDirPath(m_poConfig->GetMyGroupIdx());
  char sTmpDirPath[512] = {0};

  snprintf(sTmpDirPath, sizeof(sTmpDirPath), "%s/cp_tmp_%d", sLogStoragePath.c_str(), iSMID);
  return string(sTmpDirPath);
}

int CheckpointReceiver :: InitFilePath(const std::string & sFilePath,
				       std::string & sFormatFilePath) {
  string sNewFilePath = "/" + sFilePath + "/";
  vector<std::string> vecDirList;

  std::string sDirName;
  for (size_t i = 0; i < sNewFilePath.size(); i++) {
    if (sNewFilePath[i] == '/') {
      if (sDirName.size() > 0) {
	vecDirList.push_back(sDirName);
      }

      sDirName = "";
    } else {
      sDirName += sNewFilePath[i];
    }
  }

  sFormatFilePath = "";
  if (vecDirList.size() > 0 && vecDirList[0].size() > 0 && vecDirList[0][0] != '.') {
    sFormatFilePath += "/";
  }

  for (size_t i = 0; i < vecDirList.size(); i++){
    if (i + 1 == vecDirList.size()) {
      sFormatFilePath += vecDirList[i];
    } else {
      sFormatFilePath += vecDirList[i] + "/";
      if (m_mapHasInitDir.find(sFormatFilePath) == end(m_mapHasInitDir)) {
	int ret = CreateDir(sFormatFilePath);
        if (ret != 0) {
	  return ret;
        }

        m_mapHasInitDir[sFormatFilePath] = true;
      }
    }
  }

  PAXOSLOG_INFO << "ok, format filepath " << sFormatFilePath;
  return 0;
}

int CheckpointReceiver :: CreateDir(const std::string & sDirPath) {
  if (access(sDirPath.c_str(), F_OK) == -1) {
    if (mkdir(sDirPath.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH) == -1) {
      PAXOSLOG_ERROR << "Create dir fail, path " << sDirPath;
      return -1;
    }
  }

  return 0;
}

int CheckpointReceiver :: ReceiveCheckpoint(const CheckpointMsg & oCheckpointMsg) {
  if (oCheckpointMsg.nodeid() != m_iSenderNodeID || oCheckpointMsg.uuid() != m_llUUID) {
    PAXOSLOG_ERROR << "msg not valid, Msg.SenderNodeID " << oCheckpointMsg.nodeid()
		   << " Receiver.SenderNodeID "<< m_iSenderNodeID << " Msg.UUID "
		   << oCheckpointMsg.uuid() << " Receiver.UUID " << m_llUUID;
    return -2;
  }

  if (oCheckpointMsg.sequence() == m_llSequence) {
    PAXOSLOG_ERROR << "msg already receive, skip, Msg.Sequence " << oCheckpointMsg.sequence()
		   << " Receiver.Sequence " << m_llSequence;
    return 0;
  }

  if (oCheckpointMsg.sequence() != m_llSequence + 1) {
    PAXOSLOG_ERROR << "msg sequence wrong, Msg.Sequence " << oCheckpointMsg.sequence()
		   << " Receiver.Sequence " << m_llSequence;
    return -2;
  }

  string sFilePath = GetTmpDirPath(oCheckpointMsg.smid()) + "/" + oCheckpointMsg.filepath();
  string sFormatFilePath;
  int ret = InitFilePath(sFilePath, sFormatFilePath);
  if (ret != 0) {
    return -1;
  }

  int iFd = open(sFormatFilePath.c_str(), O_CREAT | O_RDWR | O_APPEND, S_IWRITE | S_IREAD);
  if (iFd == -1) {
    PAXOSLOG_ERROR << "open file fail, filepath " << sFormatFilePath;
    return -1;
  }

  size_t llFileOffset = lseek(iFd, 0, SEEK_END);
  if ((uint64_t)llFileOffset != oCheckpointMsg.offset()) {
    PAXOSLOG_ERROR << "file.offset "<<llFileOffset <<" not equal to msg.offset "
		   << oCheckpointMsg.offset(); 
    close(iFd);
    return -2;
  }

  size_t iWriteLen = write(iFd, oCheckpointMsg.buffer().data(), oCheckpointMsg.buffer().size());
  if (iWriteLen != oCheckpointMsg.buffer().size()) {
    PAXOSLOG_INFO << "write fail, writelen " << iWriteLen << " buffer size "
		  << oCheckpointMsg.buffer().size();
    close(iFd); 
    return -1;
  }

  m_llSequence++;
  close(iFd);

  PAXOSLOG_INFO << "END ok, writelen " << iWriteLen;
  return 0;
}
    
}

