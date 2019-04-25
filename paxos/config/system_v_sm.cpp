/*
 * Module_name: system_v_sm.cpp
 * Author: Barneyliu
 * Time: 2019-01-07
 * Description:
 *
 */

#include "system_v_sm.h"
#include "commdef.h"
#include <math.h>
#include "msg_transport.h"

namespace multipaxos
{

SystemVSM :: SystemVSM(
        const int iGroupIdx, 
        const nodeid_t iMyNodeID,
        const LogStorage * poLogStorage,
        MembershipChangeCallback pMembershipChangeCallback) 
    : m_iMyGroupIdx(iGroupIdx), m_oSystemVStore(poLogStorage), 
    m_iMyNodeID(iMyNodeID), m_pMembershipChangeCallback(pMembershipChangeCallback)
{
}

SystemVSM :: ~SystemVSM()
{
}

int SystemVSM :: Init()
{
    int ret = m_oSystemVStore.Read(m_iMyGroupIdx, m_oSystemVariables);
    if (ret != 0 && ret != 1)
    {
        return ret;
    }

    if (ret == 1)
    {
        m_oSystemVariables.set_gid(0);
        m_oSystemVariables.set_version(-1);
        PAXOSLOG_INFO << "variables not exist";
    }
    else
    {
        RefleshNodeID();
		PAXOSLOG_INFO << "OK, gourpidx " << m_iMyGroupIdx << " gid " << m_oSystemVariables.gid() 
				<< " version " << m_oSystemVariables.version();
    }

    return 0;
}

int SystemVSM :: UpdateSystemVariables(const SystemVariables & oVariables)
{
    WriteOptions oWriteOptions;
    oWriteOptions.bSync = true;

    int ret = m_oSystemVStore.Write(oWriteOptions, m_iMyGroupIdx, oVariables);
    if (ret != 0)
    {
		PAXOSLOG_ERROR << "SystemVStore::Write fail, ret " << ret;
        return -1;
    }

    m_oSystemVariables = oVariables;

    RefleshNodeID();

    return 0;
}

bool SystemVSM :: Execute(const int iGroupIdx, const uint64_t llInstanceID, const std::string & sValue, SMCtx * poSMCtx)
{
    SystemVariables oVariables;
    bool bSucc = oVariables.ParseFromArray(sValue.data(), sValue.size());
    if (!bSucc)
    {
		PAXOSLOG_ERROR << "Variables.ParseFromArray fail, bufferlen " << sValue.size();
        return false;
    }

    int * smret = nullptr;
    if (poSMCtx != nullptr && poSMCtx->m_pCtx != nullptr)
    {
        smret = (int *)poSMCtx->m_pCtx;
    }

    if (m_oSystemVariables.gid() != 0 && oVariables.gid() != m_oSystemVariables.gid())
    {
		PAXOSLOG_ERROR << "modify.gid " << oVariables.gid() << " not equal to now.gid " << m_oSystemVariables.gid();
        if (smret != nullptr) *smret = Paxos_MembershipOp_GidNotSame;
        return true;
    }

    if (oVariables.version() != m_oSystemVariables.version())
    {
		PAXOSLOG_ERROR << "modify.version " << oVariables.version() << " not equal to now.version " << m_oSystemVariables.version();
        if (smret != nullptr) 
			*smret = Paxos_MembershipOp_VersionConflit;
        return true;
    }

    oVariables.set_version(llInstanceID);
    int ret = UpdateSystemVariables(oVariables);
    if (ret != 0)
    {
        return false;
    }

	PAXOSLOG_INFO << "OK, new version " << m_oSystemVariables.version() << " gid " << m_oSystemVariables.gid();
    if (smret != nullptr) *smret = 0;

    return true;
}

//////////////////////////////////////////////////

const uint64_t SystemVSM :: GetGid() const
{
    return m_oSystemVariables.gid();
}

void SystemVSM :: GetMembership(NodeInfoList & vecNodeInfoList, uint64_t & llVersion)
{
    //must must get version first!
    llVersion = m_oSystemVariables.version();

    for (int i = 0; i < m_oSystemVariables.membership_size(); i++)
    {
        PaxosNodeInfo oNodeInfo = m_oSystemVariables.membership(i);

        NodeInfo tTmpNode(oNodeInfo.nodeid());
        vecNodeInfoList.push_back(tTmpNode);
    }
}

int SystemVSM :: Membership_OPValue(const NodeInfoList & vecNodeInfoList, const uint64_t llVersion, std::string & sOpValue)
{
    SystemVariables oVariables;
    //must must set version first!
    oVariables.set_version(llVersion);
    oVariables.set_gid(m_oSystemVariables.gid());
    
    for (auto & tNodeInfo : vecNodeInfoList)
    {
        PaxosNodeInfo * poNodeInfo = oVariables.add_membership();
        //to do, what rid?
        poNodeInfo->set_rid(0);
        poNodeInfo->set_nodeid(tNodeInfo.GetNodeID());
    }

    bool sSucc = oVariables.SerializeToString(&sOpValue);
    if (!sSucc)
    {
		PAXOSLOG_ERROR << "Variables.Serialize fail";
        return -1;
    }

    return 0;
}

int SystemVSM :: CreateGid_OPValue(const uint64_t llGid, std::string & sOpValue)
{
    SystemVariables oVariables = m_oSystemVariables;
    oVariables.set_gid(llGid);

    /*
    ** only founder need to check this. but now all is founder.
    if (oVariables.membership_size() == 0)
    {
        PLG1Err("no membership, can't create gid");
        return -1;
    }
    */
    
    bool sSucc = oVariables.SerializeToString(&sOpValue);
    if (!sSucc)
    {
		PAXOSLOG_ERROR << "Variables.Serialize fail";
        return -1;
    }

    return 0;
}
    
/////////////////////////////////////////////////

void SystemVSM :: AddNodeIDList(const NodeInfoList & vecNodeInfoList)
{
    if (m_oSystemVariables.gid() != 0)
    {
		PAXOSLOG_ERROR << "No need to add, i already have membership info.";
        return;
    }

    m_setNodeID.clear();
    m_oSystemVariables.clear_membership();

    for (auto & tNodeInfo : vecNodeInfoList)
    {
        PaxosNodeInfo * poNodeInfo = m_oSystemVariables.add_membership();
        //to do, what rid?
        poNodeInfo->set_rid(0);
        poNodeInfo->set_nodeid(tNodeInfo.GetNodeID());

        NodeInfo tTmpNode(poNodeInfo->nodeid());
    }

    RefleshNodeID();
}

void SystemVSM :: RefleshNodeID()
{
    m_setNodeID.clear();

    NodeInfoList vecNodeInfoList;
    
    for (int i = 0; i < m_oSystemVariables.membership_size(); i++)
    {
        PaxosNodeInfo oNodeInfo = m_oSystemVariables.membership(i);
        NodeInfo tTmpNode(oNodeInfo.nodeid());

		PAXOSLOG_INFO << "ip " << tTmpNode.GetIP() << " port " << tTmpNode.GetPort()
				<< " nodeid " << tTmpNode.GetNodeID();

        m_setNodeID.insert(tTmpNode.GetNodeID());

        vecNodeInfoList.push_back(tTmpNode);
    }

    if (m_pMembershipChangeCallback != nullptr)
    {
        m_pMembershipChangeCallback(m_iMyGroupIdx, vecNodeInfoList);
    }
}

const int SystemVSM :: GetNodeCount() const
{
    return (int)m_setNodeID.size();
}

const int SystemVSM :: GetMajorityCount() const
{
    return (int)(floor((double)GetNodeCount() / 2) + 1);
}

const bool SystemVSM :: IsValidNodeID(const nodeid_t iNodeID)
{
    if (m_oSystemVariables.gid() == 0)
    {
        return true;
    }
        
    return m_setNodeID.find(iNodeID) != end(m_setNodeID);
}

const bool SystemVSM :: IsIMInMembership()
{
    return m_setNodeID.find(m_iMyNodeID) != end(m_setNodeID);
}

///////////////////////////////////////////////////////////////////////////////

int SystemVSM :: GetCheckpointBuffer(std::string & sCPBuffer)
{
    if (m_oSystemVariables.version() == (uint64_t)-1
            || m_oSystemVariables.gid() == 0)
    {
        return 0;
    }
    
    bool sSucc = m_oSystemVariables.SerializeToString(&sCPBuffer);
    if (!sSucc)
    {
		PAXOSLOG_ERROR << "Variables.Serialize fail";
        return -1;
    }

    return 0;
}

int SystemVSM :: UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange)
{
    if (sCPBuffer.size() == 0)
    {
        return 0;
    }
    
    bChange = false;
    
    SystemVariables oVariables;
    bool bSucc = oVariables.ParseFromArray(sCPBuffer.data(), sCPBuffer.size());
    if (!bSucc)
    {
		PAXOSLOG_ERROR << "Variables.ParseFromArray fail, bufferlen " << sCPBuffer.size();
        return -1;
    }

    if (oVariables.version() == (uint64_t)-1)
    {
		PAXOSLOG_ERROR << "variables.version not init, this is not checkpoint";
        return -2;
    }

    if (m_oSystemVariables.gid() != 0 
            && oVariables.gid() != m_oSystemVariables.gid())
    {
		PAXOSLOG_ERROR << "gid not same, cp.gid " << oVariables.gid() << " now.gid " << m_oSystemVariables.gid();
        return -2;
    }

    if (m_oSystemVariables.version() != (uint64_t)-1 
            && oVariables.version() <= m_oSystemVariables.version())
    {
		PAXOSLOG_INFO << "lag checkpoint, no need update, cp.version " << oVariables.version() <<
				" now.version " << m_oSystemVariables.version();
        return 0;
    }

    bChange = true;
    SystemVariables oOldVariables = m_oSystemVariables;

    int ret = UpdateSystemVariables(oVariables);
    if (ret != 0)
    {
        return -1;
    }

	PAXOSLOG_INFO << "ok, cp.version " << oVariables.version() << " cp.membercount " << oVariables.membership_size() <<" old.version "
			<< oOldVariables.version() << " old.membercount " << oOldVariables.membership_size();

    return 0;
}

///////////////////////////////////////////////////////////////////////

void SystemVSM :: GetSystemVariables(SystemVariables & oVariables)
{
    oVariables = m_oSystemVariables;
}

///////////////////////////////////////////////////////////////////////

const std::set<nodeid_t> & SystemVSM :: GetMembershipMap()
{
    return m_setNodeID;
}

}

