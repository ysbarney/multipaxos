/*

*/
#pragma once

#include "event_loop.h"
#include "tcp_server.h"
#include "tcp_conn.h"
#include "buffer.h"

#include "kv_paxos.h"
#include "proto.h"
#include "def.h"
#include <mutex>
#include <iostream>
#include <fstream>
#include <stdio.h>
#include <set>
#include <time.h>
#include <tuple>

#include "buffer.hpp"
#include "zookeeper.jute.h"
#include "async_boost_log.h"

/* predefined xid's values recognized as special by the server */
#define WATCHER_EVENT_XID -1 
#define PING_XID -2
#define AUTH_XID -4
#define SET_WATCHES_XID -8


/* zookeeper event type constants */
#define CREATED_EVENT_DEF 1
#define DELETED_EVENT_DEF 2
#define CHANGED_EVENT_DEF 3
#define CHILD_EVENT_DEF 4
#define SESSION_EVENT_DEF -1
#define NOTWATCHING_EVENT_DEF -2

void GetDataReply(const multipaxos::TCPConnPtr& conn, int32_t xid, const std::string& sValue, int retCode);

class ZkServer 
{
public:
  ZkServer(multipaxos::EventLoop* loop, const std::string& addr, const multipaxos::NodeInfo & oMyNode,
	   const multipaxos::NodeInfoList & vecNodeList,
	   const multipaxos::NodeInfo & oExtraMyNode, const multipaxos::NodeInfoList & vecExtraNodeList,
	   const std::string & sKVDBPath, const std::string & sPaxosLogPath)
    : server_(loop, addr, "ZkServer", 0),
      m_oPhxKV(oMyNode, vecNodeList, oExtraMyNode, vecExtraNodeList, sKVDBPath, sPaxosLogPath) {
    server_.SetConnectionCallback(std::bind(&ZkServer::OnConnection, this, std::placeholders::_1));
    server_.SetMessageCallback(std::bind(&ZkServer::OnMessage, this, std::placeholders::_1, std::placeholders::_2));
  }

  virtual ~ZkServer() {
  }

  void Start();
  int Init();
	
private:
  void OnConnection(const multipaxos::TCPConnPtr& conn); 
  void OnMessage(const multipaxos::TCPConnPtr& conn,     multipaxos::Buffer* buf);
  void RemoveEphemeralZknode(const multipaxos::TCPConnPtr& conn);
  void handle_zk_msg(const multipaxos::TCPConnPtr& conn, const std::string& msg);
  void processRequest(const multipaxos::TCPConnPtr& conn, const std::string& buff);
  void processConnectRequest(const multipaxos::TCPConnPtr& conn, const std::string& buff);
  //void remove_ephemeral_file();
	
  typedef std::set<multipaxos::TCPConnPtr> ConnectionList;
  multipaxos::TCPServer server_;
  ConnectionList connections_;
  phxkv::PhxKV m_oPhxKV;
  const static size_t kHeaderLen = sizeof(int32_t);
};

typedef struct replyParam
{
  multipaxos::TCPConnPtr conn;
  int32_t xid;
  bool isWatch;
} ReplyParam;

typedef struct memNodeParam
{
  std::string mValue;
  uint64_t mllVersion;
}MemNodeParam;

class ZkReply 
{
public:
  ZkReply();
  virtual ~ZkReply() {}

  static ZkReply* get_instance();

  void insert_replay(const std::string& key, const multipaxos::TCPConnPtr& conn, const int32_t xid);
  void insert_watch(const std::string& key, const multipaxos::TCPConnPtr& conn);
  void insert_ephemeral_conn(const multipaxos::TCPConnPtr& conn, const std::string& key);

  void GetRelativeEphemeral(const multipaxos::TCPConnPtr& conn, std::set<std::string>& ephemeralNodes);
  void Remove_ephemeral_conn(const multipaxos::TCPConnPtr& conn);
  void request_reply(const std::string& key, const int32_t xid, const int type, const int RetCode);
  void watch_data_reply(const std::string& key, const int type);
  void watch_child_reply(const std::string& key, const int type);
  void GetDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid, const std::string& sValue, int retCode);
  void ExistReply(const multipaxos::TCPConnPtr& conn, const int32_t xid, int retCode, int32_t version);
  void PingReply(const multipaxos::TCPConnPtr& conn, const int32_t xid);
  void GetChildReply(const multipaxos::TCPConnPtr& conn, const int32_t xid,
		     const std::vector<std::string>& sValue, int retCode);
  void wait_completion();
  void notify_completion();

  void insert_ephemeral_value(const std::string & sKey, const std::string & sValue, const uint64_t llVersion);
  void Remove_ephemeral_value(const std::string& skey);
  bool check_parent_ephemeralnode(const std::string& skey);

  phxkv::KVClientRet Get_ephemeral_value(const std::string & sKey, std::string & sValue, uint64_t & llVersion);  
  phxkv::KVClientRet Set_ephemeral_value(const std::string & sKey, const std::string & sValue,
					 const uint64_t llVersion);
  phxkv::KVClientRet Create_ephemeral_value(const std::string & sKey, const std::string & sValue,
				     const uint64_t llVersion);
  phxkv::KVClientRet Del_ephemeral_value(const std::string & sKey, const uint64_t llVersion);
  phxkv::KVClientRet GetChild_ephemeral_value(const std::string& sKey, std::vector<std::string>& sValue);
	
  void SetDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid, const int type, const int RetCode);
  void CreateDataReply(const multipaxos::TCPConnPtr& conn, const std::string& key, const int32_t xid,
		       const int type, const int RetCode);
  void DeleteDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid, const int type, const int RetCode);

private:
  void watch_reply_process(const multipaxos::TCPConnPtr& conn, const std::string& key, const int type);
  void send_msg_client(const multipaxos::TCPConnPtr& conn, struct oarchive *oa);
	
private:
  std::multimap<std::string, ReplyParam> m_rparam;
  std::multimap<std::string, multipaxos::TCPConnPtr> m_rdatawatch;
  std::map<multipaxos::TCPConnPtr, std::set<std::string> > m_ephermalnode;
  std::map<std::string, MemNodeParam> m_ephkvnodes;
  pthread_cond_t cond;
  pthread_mutex_t lock;
  static ZkReply* m_replyhandle;
};
