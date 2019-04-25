/*
 * Module_name: pnode.cpp
 * Author: Barneyliu
 * Time: 2019-01-10
 * Description:
 *
 */

#include "pnode.h"

namespace multipaxos
{

PNode :: PNode()
  : m_iMyNodeID(nullnode){
}

PNode :: ~PNode()
{
  //1.step: must stop master(app) first.
  //for (auto & poMaster : m_vecMasterList)
  //{
  //    poMaster->StopMaster();
  //}
  if(m_onlyMaster) {
    m_onlyMaster->StopMaster();
    delete m_onlyMaster;
  }

  //2.step: stop proposebatch
  for (auto & poProposeBatch : m_vecProposeBatch) {
    poProposeBatch->Stop();
  }

  //3.step: stop group
  for (auto & poGroup : m_vecGroupList) {
    poGroup->Stop();
  }

  //4. step: stop network.
  m_oDefaultNetWork.StopNetWork();

  //5 .step: delete paxos instance.
  for (auto & poGroup : m_vecGroupList) {
    delete poGroup;
  }

  //6. step: delete master state machine.
  //for (auto & poMaster : m_vecMasterList)
  //{
  //    delete poMaster;
  //}
    

  //7. step: delete proposebatch;
  for (auto & poProposeBatch : m_vecProposeBatch) {
    delete poProposeBatch;
  }
}

int PNode :: InitLogStorage(const Options & oOptions, LogStorage *& poLogStorage){
  if (oOptions.poLogStorage != nullptr) {
    poLogStorage = oOptions.poLogStorage;
    PAXOSLOG_INFO << "OK, use user logstorage";
    return 0;
  }

  if (oOptions.sLogStoragePath.size() == 0) {
    PAXOSLOG_ERROR << "LogStorage Path is null";
    return -2;
  }

  int ret = m_oDefaultLogStorage.Init(oOptions.sLogStoragePath, oOptions.iGroupCount);
  if (ret != 0){
    PAXOSLOG_ERROR << "Init default logstorage fail, logpath " << oOptions.sLogStoragePath
		   << " ret " <<  ret;
    return ret;
  }

  poLogStorage = &m_oDefaultLogStorage;
  PAXOSLOG_INFO << "OK, use default logstorage";
  return 0;
}

int PNode :: InitNetWork(const Options & oOptions, NetWork *& poNetWork, NetWork *&poExtraNetWork)
{
    if (oOptions.poNetWork != nullptr)
    {
        poNetWork = oOptions.poNetWork;
		PAXOSLOG_INFO << "OK, use user network";
        return 0;
    }

    int ret = m_oDefaultNetWork.Init(
            oOptions.oMyNode.GetIP(), oOptions.oMyNode.GetPort(), oOptions.iIOThreadCount);
    if (ret != 0)
    {
		PAXOSLOG_ERROR << "init default network fail, listenip " << oOptions.oMyNode.GetIP() 
				<< " listenport " << oOptions.oMyNode.GetPort() << " ret " << ret;
        return ret;
    }

    poNetWork = &m_oDefaultNetWork;

	ret = m_oExtraNetWork.Init(oOptions.oMyExtraNode.GetIP(), oOptions.oMyExtraNode.GetPort(), 1);
	if(ret != 0) {
		PAXOSLOG_ERROR << "init extra network fail, listenip " << oOptions.oMyNode.GetIP() 
				<< " listenport " << oOptions.oMyNode.GetPort() << " ret " << ret;
        return ret;
	}

	poExtraNetWork = &m_oExtraNetWork;
    
	PAXOSLOG_INFO << "OK, use default network";

    return 0;
}

int PNode :: CheckOptions(const Options & oOptions)
{
    //init logger
    /*if (oOptions.pLogFunc != nullptr)
    {
        LOGGER->SetLogFunc(oOptions.pLogFunc);
    }
    else
    {
        LOGGER->InitLogger(oOptions.eLogLevel);
    }*/
    
    if (oOptions.poLogStorage == nullptr && oOptions.sLogStoragePath.size() == 0)
    {
		PAXOSLOG_ERROR << "no logpath and logstorage is null";
        return -2;
    }

    if (oOptions.iUDPMaxSize > 64 * 1024)
    {
		PAXOSLOG_ERROR << "udp max size " << oOptions.iUDPMaxSize << " is too large";
        return -2;
    }

    if (oOptions.iGroupCount > 200)
    {
		PAXOSLOG_ERROR << "group count " << oOptions.iGroupCount << " is too large";
        return -2;
    }

    if (oOptions.iGroupCount <= 0)
    {
		PAXOSLOG_ERROR << "group count " << oOptions.iGroupCount << " is small than zero or equal to zero";
        return -2;
    }
    
    for (auto & oFollowerNodeInfo : oOptions.vecFollowerNodeInfoList)
    {
        if (oFollowerNodeInfo.oMyNode.GetNodeID() == oFollowerNodeInfo.oFollowNode.GetNodeID())
        {
			PAXOSLOG_ERROR << "self node ip " << oFollowerNodeInfo.oMyNode.GetIP() << " port " << oFollowerNodeInfo.oMyNode.GetPort()
						<< " equal to follow node";
            return -2;
        }
    }

    for (auto & oGroupSMInfo : oOptions.vecGroupSMInfoList)
    {
        if (oGroupSMInfo.iGroupIdx >= oOptions.iGroupCount)
        {
			PAXOSLOG_ERROR << "SM GroupIdx " << oGroupSMInfo.iGroupIdx << " large than GroupCount " << oOptions.iGroupCount;
            return -2;
        }
    }

    return 0;
}

void PNode :: InitStateMachine(const Options & oOptions)
{
    for (auto & oGroupSMInfo : oOptions.vecGroupSMInfoList)
    {
        for (auto & poSM : oGroupSMInfo.vecSMList)
        {
            AddStateMachine(oGroupSMInfo.iGroupIdx, poSM);
        }
    }
}

void PNode :: RunMaster(const Options & oOptions)
{
    for (auto & oGroupSMInfo : oOptions.vecGroupSMInfoList)
    {
        //check if need to run master.
        if (oGroupSMInfo.bIsUseMaster)
        {
            if (!m_vecGroupList[oGroupSMInfo.iGroupIdx]->GetConfig()->IsIMFollower())
            {
                //m_vecMasterList[oGroupSMInfo.iGroupIdx]->RunMaster();
                if(oGroupSMInfo.iGroupIdx == 0)
					m_onlyMaster->RunMaster();
            }
            else
            {
				PAXOSLOG_INFO << "I'm follower, not run master damon.";
            }
        }
    }
}

void PNode :: RunProposeBatch()
{
    /*for (auto & poProposeBatch : m_vecProposeBatch)
    {
        poProposeBatch->Start();
    }*/
}

int PNode :: Init(const Options & oOptions, NetWork *& poNetWork, NetWork *& poExtraNetWork)
{
    int ret = CheckOptions(oOptions);
    if (ret != 0)
    {
		PAXOSLOG_ERROR << "CheckOptions fail, ret " << ret;
        return ret;
    }

    m_iMyNodeID = oOptions.oMyNode.GetNodeID();
	m_oOptions = (Options &)oOptions;
	
    //step1 init logstorage
    LogStorage * poLogStorage = nullptr;
    ret = InitLogStorage(oOptions, poLogStorage);
    if (ret != 0)
    {
        return ret;
    }

    //step2 init network
    ret = InitNetWork(oOptions, poNetWork, poExtraNetWork);
    if (ret != 0)
    {
        return ret;
    }

    //step3 build masterlist
    /*for (int iGroupIdx = 0; iGroupIdx < oOptions.iGroupCount; iGroupIdx++)
    {
        MasterMgr * poMaster = new MasterMgr(this, iGroupIdx, poLogStorage, oOptions.pMasterChangeCallback);
        assert(poMaster != nullptr);
        m_vecMasterList.push_back(poMaster);

        ret = poMaster->Init();
        if (ret != 0)
        {
            return ret;
        }
    }*/
    
    m_onlyMaster = new MasterMgr(this, 0, poLogStorage, oOptions, oOptions.pMasterChangeCallback);
    assert(m_onlyMaster != nullptr);
    //m_vecMasterList.push_back(poMaster);

    ret = m_onlyMaster->Init();
    if (ret != 0)
    {
        return ret;
    }

    //step4 build grouplist
    for (int iGroupIdx = 0; iGroupIdx < oOptions.iGroupCount; iGroupIdx++)
    {
        Group * poGroup = new Group(this, poLogStorage, poNetWork, m_onlyMaster->GetMasterSM(), iGroupIdx, oOptions);
        assert(poGroup != nullptr);
        m_vecGroupList.push_back(poGroup);
    }

    //step5 build batchpropose
    if (oOptions.bUseBatchPropose)
    {
        for (int iGroupIdx = 0; iGroupIdx < oOptions.iGroupCount; iGroupIdx++)
        {
            ProposeBatch * poProposeBatch = new ProposeBatch(iGroupIdx, this, &m_oNotifierPool);
            assert(poProposeBatch != nullptr);
            m_vecProposeBatch.push_back(poProposeBatch);
        }
    }

    //step6 init statemachine
    InitStateMachine(oOptions);    

    //step7 parallel init group
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->StartInit();
    }

    for (auto & poGroup : m_vecGroupList)
    {
        int initret = poGroup->GetInitRet();
        if (initret != 0)
        {
            ret = initret;
        }
    }

    if (ret != 0)
    {
        return ret;
    }

    //last step. must init ok, then should start threads.
    //because that stop threads is slower, if init fail, we need much time to stop many threads.
    //so we put start threads in the last step.
    for (auto & poGroup : m_vecGroupList)
    {
        //start group's thread first.
        poGroup->Start();
    }
    RunMaster(oOptions);
    RunProposeBatch();

	PAXOSLOG_INFO << "OK";

    return 0;
}

bool PNode :: CheckGroupID(const int iGroupIdx)
{
    if (iGroupIdx < 0 || iGroupIdx >= (int)m_vecGroupList.size())
    {
        return false;
    }

    return true;
}

int PNode::MasterPropose(const int iGroupIdx, const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
	string sPackSMIDValue = sValue;
	if(poSMCtx) {
		m_vecGroupList[iGroupIdx]->GetInstance()->GetSMFac()->PackPaxosValue(sPackSMIDValue, poSMCtx->m_iSMID);
	}
	return m_vecGroupList[iGroupIdx]->GetInstance()->GetQueueProposer()->AddMessage(sPackSMIDValue.c_str(), (int)sPackSMIDValue.size());
}

int PNode :: Propose(const int iGroupIdx, const std::string & sValue, uint64_t & llInstanceID)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

	int ret = 0;
	if(IsIMMaster(iGroupIdx))
    	ret = MasterPropose(iGroupIdx, sValue, llInstanceID, nullptr);
	else {
		ret = Forward(iGroupIdx, sValue, llInstanceID, nullptr);
	}
	return ret;
}

int PNode :: Propose(const int iGroupIdx, const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

	int ret = 0;
	if(IsIMMaster(iGroupIdx)) {
		PAXOSLOG_INFO << "===Master Propose iGroupIdx" << iGroupIdx << " msg " << sValue.size();
		ret = MasterPropose(iGroupIdx, sValue, llInstanceID, poSMCtx);
	} else {
		ret = Forward(iGroupIdx, sValue, llInstanceID, poSMCtx);
	}
	return ret;
}

int PNode::Forward(const int iGroupIdx, const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
	string sPackSMIDValue = sValue;
	if(poSMCtx) {
		m_vecGroupList[iGroupIdx]->GetInstance()->GetSMFac()->PackPaxosValue(sPackSMIDValue, poSMCtx->m_iSMID);
	}

	std::string sendValue;
	int sGroupIdx = -1, sForwardMaster = 0;
	if (GetMasterExtra(iGroupIdx).GetNodeID() == nullnode) {
		sGroupIdx = 0;
		sForwardMaster = 1;
	} else 
		sGroupIdx = iGroupIdx;
	
	if(PackExtraMsg(sGroupIdx, 0, sPackSMIDValue, sendValue)) {
		PAXOSLOG_ERROR << "Pack forward message failed";
		return -1;
	}

	if(sForwardMaster) {
		if(GetHBMaster(iGroupIdx).GetNodeID() == nullnode) {
			PAXOSLOG_ERROR << "Get iGroupIdx " << iGroupIdx << " Master IP and Port failed";
			return -1;
		}
		return m_oExtraNetWork.SendMessageTCP(iGroupIdx, GetHBMaster(iGroupIdx).GetIP(), GetHBMaster(iGroupIdx).GetPort(), sendValue);
	} 
	
	PAXOSLOG_INFO << "===Forward Msg to master nodeid " << GetMasterExtra(iGroupIdx).GetNodeID() << " iGroupIdx " << iGroupIdx << " msg " << sendValue.size();
	return m_oExtraNetWork.SendMessageTCP(iGroupIdx, GetMasterExtra(iGroupIdx).GetIP(), GetMasterExtra(iGroupIdx).GetPort(), sendValue);
}

int PNode::ReportStatus(const int iGroupIdx, const std::string & sValue, uint64_t & llInstanceID, SMCtx * poSMCtx)
{
	if(!IsIMHBMaster(iGroupIdx)) {
		string sPackSMIDValue = sValue;
		if(poSMCtx) {
			m_vecGroupList[iGroupIdx]->GetInstance()->GetSMFac()->PackPaxosValue(sPackSMIDValue, poSMCtx->m_iSMID);
		}

		std::string sendValue;
		if(PackExtraMsg(iGroupIdx, 1, sPackSMIDValue, sendValue)) {
			PAXOSLOG_ERROR << "Pack report status message failed";
			return -1;
		}

		if(GetHBMaster(iGroupIdx).GetNodeID() == nullnode) {
			PAXOSLOG_ERROR << "Get iGroupIdx " << iGroupIdx << " Master Hb IP and Port failed";
			return -1;
		}
		return m_oExtraNetWork.SendMessageTCP(iGroupIdx, GetHBMaster(iGroupIdx).GetIP(), GetHBMaster(iGroupIdx).GetPort(), sendValue);
	}

	return 0;
}

int PNode::PackExtraMsg(const int iGroupIdx, const int msgType, const std::string& sValue, std::string& packMsg)
{
	char sMsgType[GROUPIDXLEN] = {0};
	memcpy(sMsgType, &msgType, sizeof(sMsgType));
	
	char sGroupIdx[GROUPIDXLEN] = {0};
	memcpy(sGroupIdx, &iGroupIdx, sizeof(sGroupIdx));
	packMsg = string(sMsgType, sizeof(sMsgType)) + string(sGroupIdx, sizeof(sGroupIdx)) + sValue;
	return 0;
}

const uint64_t PNode :: GetNowInstanceID(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return (uint64_t)-1;
    }

    return m_vecGroupList[iGroupIdx]->GetInstance()->GetNowInstanceID();
}

const uint64_t PNode :: GetMinChosenInstanceID(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return (uint64_t)-1;
    }

    return m_vecGroupList[iGroupIdx]->GetInstance()->GetMinChosenInstanceID();
}

int PNode :: OnReceiveMessage(const unsigned short iport, const char * pcMessage, const int iMessageLen)
{
	int ret = 0;
	//PAXOSLOG_INFO << "OnReceiveMessage iport " << iport << " oMyNode port " << m_oOptions.oMyNode.GetPort() <<" iMessageLen " << iMessageLen;
    if(iport == m_oOptions.oMyNode.GetPort())
		ret = OnRecvPaxosMessage(pcMessage, iMessageLen);
	else
		ret = OnRecvExtraMessage(pcMessage, iMessageLen);
	return ret;
}

int PNode::OnRecvPaxosMessage(const char * pcMessage, const int iMessageLen)
{
	if (pcMessage == nullptr || iMessageLen <= 0)
    {
        PAXOSLOG_ERROR << "Message size " << iMessageLen << " to small, not valid.";
        return -2;
    }
    
    int iGroupIdx = -1;
    memcpy(&iGroupIdx, pcMessage, GROUPIDXLEN);
	
    if (!CheckGroupID(iGroupIdx))
    {
		PAXOSLOG_ERROR << "Message groupid " << iGroupIdx << " wrong, groupsize " << m_vecGroupList.size();
        return Paxos_GroupIdxWrong;
    }

 	return m_vecGroupList[iGroupIdx]->GetInstance()->OnReceiveMessage(pcMessage, iMessageLen);
}

int PNode :: OnRecvExtraMessage(const char * pcMessage, const int iMessageLen)
{
    if (pcMessage == nullptr || iMessageLen <= 0)
    {
		PAXOSLOG_ERROR << "Message size " << iMessageLen << " to small, not valid.";
		return -2;
    }

	int msg_type = -1;
	memcpy(&msg_type, pcMessage, sizeof(int));
	if(msg_type == 0) {
	    int iGroupIdx = -1;
		//int mixIdx;
	    memcpy(&iGroupIdx, pcMessage+sizeof(int), GROUPIDXLEN);
		
	    if (!CheckGroupID(iGroupIdx))
	    {
			PAXOSLOG_ERROR << "Message groupid " << iGroupIdx << " wrong, groupsize " << m_vecGroupList.size();
	        return Paxos_GroupIdxWrong;
	    }
		PAXOSLOG_DEBUG << "Recv proposer message size "  << iMessageLen - 2*sizeof(int);
	 	return m_vecGroupList[iGroupIdx]->GetInstance()->GetQueueProposer()->AddMessage(pcMessage+ 2*sizeof(int), iMessageLen - 2*sizeof(int));
	} else {
		return m_onlyMaster->GetMetaDispatch()->AddMessage(pcMessage+ 2*sizeof(int), iMessageLen - 2 * sizeof(int));
	}
	
}


void PNode :: AddStateMachine(StateMachine * poSM)
{
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->AddStateMachine(poSM);
    }
}

void PNode :: AddStateMachine(const int iGroupIdx, StateMachine * poSM)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return;
    }
    
    m_vecGroupList[iGroupIdx]->AddStateMachine(poSM);
}

const nodeid_t PNode :: GetMyNodeID() const
{
    return m_iMyNodeID;
}

void PNode :: SetTimeoutMs(const int iTimeoutMs)
{
    /*for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCommitter()->SetTimeoutMs(iTimeoutMs);
    }*/
}

int PNode::SetMinChosenInstanceID(const int iGroupIdx, const uint64_t& llInstanceID) {
  if (!CheckGroupID(iGroupIdx)) {
    return -1;
  }
  
  WriteOptions oWriteOptions;
  oWriteOptions.bSync = false;
  return m_oDefaultLogStorage.SetMinChosenInstanceID(oWriteOptions, iGroupIdx, llInstanceID);
}

////////////////////////////////////////////////////////////////////////

void PNode :: SetHoldPaxosLogCount(const uint64_t llHoldCount)
{
    /*for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCheckpointCleaner()->SetHoldPaxosLogCount(llHoldCount);
    }*/
}

void PNode :: PauseCheckpointReplayer()
{
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCheckpointReplayer()->Pause();
    }
}

void PNode :: ContinueCheckpointReplayer()
{
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCheckpointReplayer()->Continue();
    }
}

void PNode :: PausePaxosLogCleaner()
{
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCheckpointCleaner()->Pause();
    }
}

void PNode :: ContinuePaxosLogCleaner()
{
    for (auto & poGroup : m_vecGroupList)
    {
        poGroup->GetCheckpointCleaner()->Continue();
    }
}

///////////////////////////////////////////////////////

int PNode :: ProposalMembership(
        SystemVSM * poSystemVSM, 
        const int iGroupIdx, 
        const NodeInfoList & vecNodeInfoList, 
        const uint64_t llVersion)
{
    string sOpValue;
    int ret = poSystemVSM->Membership_OPValue(vecNodeInfoList, llVersion, sOpValue);
    if (ret != 0)
    {
        return Paxos_SystemError;
    }

    SMCtx oCtx;
    int smret = -1;
    oCtx.m_iSMID = SYSTEM_V_SMID;
    oCtx.m_pCtx = (void *)&smret;

    uint64_t llInstanceID = 0;
    ret = Propose(iGroupIdx, sOpValue, llInstanceID, &oCtx);
    if (ret != 0)
    {
        return ret;
    }

    return smret;
}

int PNode :: AddMember(const int iGroupIdx, const NodeInfo & oNode)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    SystemVSM * poSystemVSM = m_vecGroupList[iGroupIdx]->GetConfig()->GetSystemVSM();

    if (poSystemVSM->GetGid() == 0)
    {
        return Paxos_MembershipOp_NoGid;
    }

    uint64_t llVersion = 0;
    NodeInfoList vecNodeInfoList;
    poSystemVSM->GetMembership(vecNodeInfoList, llVersion);

    for (auto & oNodeInfo : vecNodeInfoList)
    {
        if (oNodeInfo.GetNodeID() == oNode.GetNodeID())
        {
            return Paxos_MembershipOp_Add_NodeExist;
        }
    }

    vecNodeInfoList.push_back(oNode);

    return ProposalMembership(poSystemVSM, iGroupIdx, vecNodeInfoList, llVersion);
}

int PNode :: RemoveMember(const int iGroupIdx, const NodeInfo & oNode)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    SystemVSM * poSystemVSM = m_vecGroupList[iGroupIdx]->GetConfig()->GetSystemVSM();

    if (poSystemVSM->GetGid() == 0)
    {
        return Paxos_MembershipOp_NoGid;
    }

    uint64_t llVersion = 0;
    NodeInfoList vecNodeInfoList;
    poSystemVSM->GetMembership(vecNodeInfoList, llVersion);

    bool bNodeExist = false;
    NodeInfoList vecAfterNodeInfoList;
    for (auto & oNodeInfo : vecNodeInfoList)
    {
        if (oNodeInfo.GetNodeID() == oNode.GetNodeID())
        {
            bNodeExist = true;
        }
        else
        {
            vecAfterNodeInfoList.push_back(oNodeInfo);
        }
    }

    if (!bNodeExist)
    {
        return Paxos_MembershipOp_Remove_NodeNotExist;
    }

    return ProposalMembership(poSystemVSM, iGroupIdx, vecAfterNodeInfoList, llVersion);
}

int PNode :: ChangeMember(const int iGroupIdx, const NodeInfo & oFromNode, const NodeInfo & oToNode)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    SystemVSM * poSystemVSM = m_vecGroupList[iGroupIdx]->GetConfig()->GetSystemVSM();

    if (poSystemVSM->GetGid() == 0)
    {
        return Paxos_MembershipOp_NoGid;
    }

    uint64_t llVersion = 0;
    NodeInfoList vecNodeInfoList;
    poSystemVSM->GetMembership(vecNodeInfoList, llVersion);

    NodeInfoList vecAfterNodeInfoList;
    bool bFromNodeExist = false;
    bool bToNodeExist = false;
    for (auto & oNodeInfo : vecNodeInfoList)
    {
        if (oNodeInfo.GetNodeID() == oFromNode.GetNodeID())
        {
            bFromNodeExist = true;
            continue;
        }
        else if (oNodeInfo.GetNodeID() == oToNode.GetNodeID())
        {
            bToNodeExist = true;
            continue;
        }

        vecAfterNodeInfoList.push_back(oNodeInfo);
    }

    if ((!bFromNodeExist) && bToNodeExist)
    {
        return Paxos_MembershipOp_Change_NoChange;
    }

    vecAfterNodeInfoList.push_back(oToNode);

    return ProposalMembership(poSystemVSM, iGroupIdx, vecAfterNodeInfoList, llVersion);
}

int PNode :: ShowMembership(const int iGroupIdx, NodeInfoList & vecNodeInfoList)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    SystemVSM * poSystemVSM = m_vecGroupList[iGroupIdx]->GetConfig()->GetSystemVSM();

    uint64_t llVersion = 0;
    poSystemVSM->GetMembership(vecNodeInfoList, llVersion);

    return 0;
}

//////////////////////////////////////////////////////////////////////////////

const NodeInfo PNode :: GetMaster(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return NodeInfo(nullnode);
    }

	return NodeInfo(m_onlyMaster->GetMasterSM()->GetMaster(iGroupIdx));
}

const NodeInfo PNode :: GetMasterExtra(const int iGroupIdx)
{
	std::string masterip = GetMaster(iGroupIdx).GetIP();
	for(size_t i=0; i<m_oOptions.vecExtraNodeInfoList.size(); i++) {
		NodeInfo nodeID = m_oOptions.vecExtraNodeInfoList[i];
		if(nodeID.GetIP() == masterip) {
			return nodeID;
		}
	}
	return NodeInfo(nullnode);
}

const NodeInfo PNode :: GetMasterWithVersion(const int iGroupIdx, uint64_t & llVersion) 
{
    if (!CheckGroupID(iGroupIdx))
    {
        return NodeInfo(nullnode);
    }

    //return NodeInfo(m_vecMasterList[iGroupIdx]->GetMasterSM()->GetMasterWithVersion(llVersion));
    return NodeInfo(m_onlyMaster->GetMasterSM()->GetMasterWithVersion(llVersion));
}

const NodeInfo PNode :: GetHBMaster(const int iGroupIdx) 
{
	uint64_t llVersion = 0;
    std::string masterip = GetMasterWithVersion(iGroupIdx, llVersion).GetIP();
	for(size_t i=0; i<m_oOptions.vecExtraNodeInfoList.size(); i++) {
		NodeInfo nodeID = m_oOptions.vecExtraNodeInfoList[i];
		if(nodeID.GetIP() == masterip) {
			return nodeID;
		}
	}
	return NodeInfo(nullnode);
}

const bool PNode :: IsIMHBMaster(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return false;
    }

	uint64_t llVersion = 0;
    nodeid_t mNodeID = m_onlyMaster->GetMasterSM()->GetMasterWithVersion(llVersion);
	
    return m_iMyNodeID == mNodeID;
}



const bool PNode :: IsIMMaster(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return false;
    }

    return m_onlyMaster->GetMasterSM()->IsIMMaster(iGroupIdx);
}

int PNode :: SetMasterLease(const int iGroupIdx, const int iLeaseTimeMs)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    //m_vecMasterList[iGroupIdx]->SetLeaseTime(iLeaseTimeMs);
    m_onlyMaster->SetLeaseTime(iLeaseTimeMs);
    return 0;
}

int PNode :: DropMaster(const int iGroupIdx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    //m_vecMasterList[iGroupIdx]->DropMaster();
    m_onlyMaster->DropMaster();
    return 0;
}

/////////////////////////////////////////////////////////////////////

void PNode :: SetMaxHoldThreads(const int iGroupIdx, const int iMaxHoldThreads)
{
    /*if (!CheckGroupID(iGroupIdx))
    {
        return;
    }

    m_vecGroupList[iGroupIdx]->GetCommitter()->SetMaxHoldThreads(iMaxHoldThreads);*/
}

void PNode :: SetProposeWaitTimeThresholdMS(const int iGroupIdx, const int iWaitTimeThresholdMS)
{
    /*if (!CheckGroupID(iGroupIdx))
    {
        return;
    }

    m_vecGroupList[iGroupIdx]->GetCommitter()->SetProposeWaitTimeThresholdMS(iWaitTimeThresholdMS);*/
}

void PNode :: SetLogSync(const int iGroupIdx, const bool bLogSync)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return;
    }

    m_vecGroupList[iGroupIdx]->GetConfig()->SetLogSync(bLogSync);
}

//////////////////////////////////////////////////////////////////////

int PNode :: GetInstanceValue(const int iGroupIdx, const uint64_t llInstanceID, 
        std::vector<std::pair<std::string, int> > & vecValues)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    string sValue;
    int iSMID = 0;
    int ret = m_vecGroupList[iGroupIdx]->GetInstance()->GetInstanceValue(llInstanceID, sValue, iSMID);
    if (ret != 0)
    {
        return ret;
    }

    if (iSMID == BATCH_PROPOSE_SMID)
    {
        BatchPaxosValues oBatchValues;
        bool bSucc = oBatchValues.ParseFromArray(sValue.data(), sValue.size());
        if (!bSucc)
        {
            return Paxos_SystemError;
        }

        for (int i = 0; i < oBatchValues.values_size(); i++)
        {
            const PaxosValue & oValue = oBatchValues.values(i);
            vecValues.push_back(make_pair(oValue.value(), oValue.smid()));
        }
    }
    else
    {
        vecValues.push_back(make_pair(sValue, iSMID));
    }

    return 0;
}

//////////////////////////////////////////////////////////////////////////

int PNode :: BatchPropose(const int iGroupIdx, const std::string & sValue, 
        uint64_t & llInstanceID, uint32_t & iBatchIndex)
{
    return BatchPropose(iGroupIdx, sValue, llInstanceID, iBatchIndex, nullptr);
}

int PNode :: BatchPropose(const int iGroupIdx, const std::string & sValue, 
        uint64_t & llInstanceID, uint32_t & iBatchIndex, SMCtx * poSMCtx)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return Paxos_GroupIdxWrong;
    }

    if (m_vecProposeBatch.size() == 0)
    {
        return Paxos_SystemError;
    }

    return m_vecProposeBatch[iGroupIdx]->Propose(sValue, llInstanceID, iBatchIndex, poSMCtx);
}

void PNode :: SetBatchCount(const int iGroupIdx, const int iBatchCount)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return;
    }

    if (m_vecProposeBatch.size() == 0)
    {
        return;
    }

    m_vecProposeBatch[iGroupIdx]->SetBatchCount(iBatchCount);
}

void PNode :: SetBatchDelayTimeMs(const int iGroupIdx, const int iBatchDelayTimeMs)
{
    if (!CheckGroupID(iGroupIdx))
    {
        return;
    }

    if (m_vecProposeBatch.size() == 0)
    {
        return;
    }

    m_vecProposeBatch[iGroupIdx]->SetBatchDelayTimeMs(iBatchDelayTimeMs);
}
    
}


