/*
 * Module_name: main.cpp
 * Author: Barneyliu
 * Time: 2018-12-29
 * Description:
 *
 */
#include "zk_server.h"

using namespace multipaxos;

int parse_ipport(const char * pcStr, NodeInfo & oNodeInfo)
{
	 char sIP[32] = {0};
	 int iPort = -1;

	 int count = sscanf(pcStr, "%[^':']:%d", sIP, &iPort);
	 if (count != 2)
	 {
		 return -1;
	 }

	 oNodeInfo.SetIPPort(sIP, iPort);

	 return 0;
}

int parse_ipport_list(const char * pcStr, NodeInfoList & vecNodeInfoList)
{
	 string sTmpStr;
	 int iStrLen = strlen(pcStr);

	 for (int i = 0; i < iStrLen; i++)
	 {
		 if (pcStr[i] == ',' || i == iStrLen - 1)
		 {
			 if (i == iStrLen - 1 && pcStr[i] != ',')
			 {
				 sTmpStr += pcStr[i];
			 }
			 
			 NodeInfo oNodeInfo;
			 int ret = parse_ipport(sTmpStr.c_str(), oNodeInfo);
			 if (ret != 0)
			 {
				 return ret;
			 }

			 vecNodeInfoList.push_back(oNodeInfo);

			 sTmpStr = "";
		 }
		 else
		 {
			 sTmpStr += pcStr[i];
		 }
	 }

	 return 0;
}

int main(int argc, char **argv)
{
	if (argc < 6)
    {
        printf("%s <grpc myip:myport> <paxos myip:myport> <hb myip:myport> <node0_ip:node_0port,node1_ip:node1_port,node2_ip:node2_port,...> <node0_ip:node_0hport,node1_ip:node1_hport,node2_ip:node2_hport,...> <kvdb storagepath> <paxoslog storagepath>\n", argv[0]);
        return -1;
    }

	async_boost_log aboost_log;
	aboost_log.initialize("../conf/logconfig.ini");

    string sServerAddress = argv[1];

    NodeInfo oMyNode;
    if (parse_ipport(argv[2], oMyNode) != 0)
    {
        printf("parse paxos myip:myport fail\n");
        return -1;
    }

	NodeInfo oExtraMyNode;
    if (parse_ipport(argv[3], oExtraMyNode) != 0)
    {
        printf("parse hb myip:myport fail\n");
        return -1;
    }

    NodeInfoList vecNodeInfoList;
    if (parse_ipport_list(argv[4], vecNodeInfoList) != 0)
    {
        printf("parse ip/port list fail\n");
        return -1;
    }

	NodeInfoList vecExtraNodeInfoList;
    if (parse_ipport_list(argv[5], vecExtraNodeInfoList) != 0)
    {
        printf("parse ip/port list fail\n");
        return -1;
    }
	
	string sKVDBPath = argv[6];
    string sPaxosLogPath = argv[7];
	
	multipaxos::EventLoop loop;
	ZkServer server(&loop, sServerAddress, oMyNode, vecNodeInfoList, oExtraMyNode, vecExtraNodeInfoList, sKVDBPath, sPaxosLogPath);
	server.Init();
	ZKLOG_INFO << "server init ok.............................";
	
	server.Start();
	loop.Run();

	return 0;
}
