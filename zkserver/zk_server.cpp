#include "zk_server.h"

using namespace phxkv;

static int64_t g_zxid = 0;

const int ZOO_EPHEMERAL = 1 << 0;
const int ZOO_SEQUENCE = 1 << 1;

/* zookeeper state constants */
#define EXPIRED_SESSION_STATE_DEF -112
#define AUTH_FAILED_STATE_DEF -113
#define CONNECTING_STATE_DEF 1
#define ASSOCIATING_STATE_DEF 2
#define CONNECTED_STATE_DEF 3


int ZkServer::Init(){
  return m_oPhxKV.RunPaxos();
}

void ZkServer::Start(){
  server_.Init();
  server_.Start();
}

void ZkServer::OnConnection(const multipaxos::TCPConnPtr& conn) {
  //std::cout << conn->AddrToString() << " is " << (conn->IsConnected() ? "UP" : "DOWN") << std::endl;
  if (!conn->IsConnected()) {
    connections_.erase(conn);
    ZkReply::get_instance()->notify_completion();
    RemoveEphemeralZknode(conn);
  }
}

void ZkServer::OnMessage(const multipaxos::TCPConnPtr& conn, multipaxos::Buffer* buf){
  while (buf->size() >= kHeaderLen) {
    const int32_t len = buf->PeekInt32();
    if (len < 0) {
      //LOG_ERROR << "Invalid length " << len;
      conn->Close();
      break;
    }

    if (buf->size() >= len + kHeaderLen) {
      buf->Skip(kHeaderLen);
      std::string message(buf->NextString(len));
      handle_zk_msg(conn, message);
      break;
    } else {
      break;
    }
  }
}

void ZkServer::handle_zk_msg(const multipaxos::TCPConnPtr& conn, const std::string& msg){
  if (connections_.find(conn) == connections_.end()) {
    //std::cout << "new conn " << conn->fd() << " status first" << std::endl;
    processConnectRequest(conn, msg);
    connections_.insert(conn);
  } else {
    //std::cout << "new conn " << conn->fd() << " status second" << std::endl;
    processRequest(conn, msg);
  }
  g_zxid++;
}


void ZkServer::processConnectRequest(const multipaxos::TCPConnPtr& conn, const std::string& buff) {
  struct ConnectRequest connReq;
  struct iarchive *ia = create_buffer_iarchive((char*) buff.c_str(), (int)buff.size());
  deserialize_ConnectRequest(ia, "connReq", &connReq);

  ZKLOG_DEBUG << "protocolVersion " << connReq.protocolVersion
	      << " lastZxidSeen " << connReq.lastZxidSeen
	      << " timeOut " << connReq.timeOut
	      <<" sessionId "<< connReq.sessionId
	      <<" passwd " << connReq.passwd.buff << std::endl;

  int64_t session_id = (int64_t)time(NULL);
  struct buffer pwd;
  pwd.len = 16;
  pwd.buff = (char*)malloc(pwd.len);
  pwd.buff[0] = 0x12;
  pwd.buff[6] = 0x12;
  pwd.buff[3] = 0x12;
  pwd.buff[8] = 0x12;
  pwd.buff[9] = 0x12;

  struct oarchive *oa;
  oa = create_buffer_oarchive();
  struct ConnectResponse connResp = {.protocolVersion=connReq.protocolVersion, .timeOut=connReq.timeOut,
				     .sessionId=session_id, .passwd=pwd};
  serialize_ConnectResponse(oa, "connResp", &connResp);

  char buf[256] = {0};
  int buffer_len = htonl(get_buffer_len(oa));
  memcpy(buf, &buffer_len, 4);
  memcpy(buf+4, get_buffer(oa), get_buffer_len(oa));
  write(conn->fd(), buf, get_buffer_len(oa)+4);
  free(pwd.buff);
  close_buffer_oarchive(&oa, get_buffer_len(oa));
  close_buffer_iarchive(&ia);
}

void ZkServer::processRequest(const multipaxos::TCPConnPtr& conn, const std::string& buff){
  struct RequestHeader hdr;
  struct iarchive *ia = create_buffer_iarchive((char*) buff.c_str(), (int)buff.size());
  deserialize_RequestHeader(ia, "hdr", &hdr);

  ZKLOG_INFO << "processRequest xid: " << hdr.xid << " type: " << hdr.type;

  switch(hdr.type) {
  case SETDATA_OP :{ //SETDATA_OP
    struct SetDataRequest SetDataReq;
    deserialize_SetDataRequest(ia, "SetDataReq", &SetDataReq);

    ZkReply::get_instance()->insert_replay(SetDataReq.path, conn, hdr.xid);
    m_oPhxKV.Set(SetDataReq.path, SetDataReq.data.buff, hdr.xid, SetDataReq.version);
    if(!m_oPhxKV.IsIMMaster(SetDataReq.path))
      ZkReply::get_instance()->wait_completion();
  }
  break;

  case GETDATA_OP : {//GETDATA_OP
    struct GetDataRequest GetDataReq;
    deserialize_GetDataRequest(ia, "GetDateReq", &GetDataReq);

    ZKLOG_INFO << " key " << GetDataReq.path << " watch " << GetDataReq.watch << std::endl;
    if(GetDataReq.watch)
      ZkReply::get_instance()->insert_watch(GetDataReq.path, conn);

    std::string sValue;
    uint64_t llVersion;
    phxkv::PhxKVStatus ret = m_oPhxKV.GetLocal(GetDataReq.path, sValue, llVersion);
    int retCode = -1;
    if(ret == phxkv::PhxKVStatus::SUCC)
      retCode = 0;
    else if(ret == phxkv::PhxKVStatus::KEY_NOTEXIST)
      retCode = -101;

    ZkReply::get_instance()->GetDataReply(conn, hdr.xid, sValue, retCode);
  }
  break;

  case DELETE_OP: {
    struct DeleteRequest DelReq;
    deserialize_DeleteRequest(ia, "DelReq", &DelReq);

    ZkReply::get_instance()->insert_replay(DelReq.path, conn, hdr.xid);
    m_oPhxKV.Delete(DelReq.path, hdr.xid, DelReq.version);

    if(!m_oPhxKV.IsIMMaster(DelReq.path))
      ZkReply::get_instance()->wait_completion();
  }
  break;

  case CREATE_OP: {
    struct CreateRequest CreateReq;
    deserialize_CreateRequest(ia, "CreateReq", &CreateReq);

    std::string skey = CreateReq.path;
    if(CreateReq.flags & ZOO_SEQUENCE) {
      char ctaskid[16]={0};
      snprintf(ctaskid,16,"%010d",g_zxid);
      std::string id = ctaskid;
      skey = skey+id;
    }

    uint32_t node_type = 0;
    if(CreateReq.flags & ZOO_EPHEMERAL) {
      ZkReply::get_instance()->insert_ephemeral_conn(conn, skey);
      node_type = 1;
    }

    if(ZkReply::get_instance()->check_parent_ephemeralnode(skey)) {
      ZkReply::get_instance()->CreateDataReply(conn, skey, hdr.xid,
					       KVOperatorType_CREATE, ZNOCHILDRENFOREPHEMERALS);
      return;
    }

    ZkReply::get_instance()->insert_replay(skey, conn, hdr.xid);
    m_oPhxKV.Create(skey, CreateReq.data.buff, node_type, hdr.xid, -1);

    if(!m_oPhxKV.IsIMMaster(skey))
      ZkReply::get_instance()->wait_completion();
  }
  break;

  case EXISTS_OP: {
    struct ExistsRequest ExistReq;
    deserialize_ExistsRequest(ia, "ExistReq", &ExistReq);

    ZKLOG_INFO << " key " << ExistReq.path << " watch " << ExistReq.watch << std::endl;

    if(ExistReq.watch)
      ZkReply::get_instance()->insert_watch(ExistReq.path, conn);


    std::string sValue;
    uint64_t llVersion;
    phxkv::PhxKVStatus ret = m_oPhxKV.GetLocal(ExistReq.path, sValue, llVersion);
    int retCode = -1;
    if(ret == phxkv::PhxKVStatus::SUCC)
      retCode = 0;
    else if(ret == phxkv::PhxKVStatus::KEY_NOTEXIST)
      retCode = -101;

    ZkReply::get_instance()->ExistReply(conn, hdr.xid, retCode, (int32_t)llVersion);
  }
  break;

  case PING_OP:	{
    ZkReply::get_instance()->PingReply(conn, hdr.xid);
  }
  break;

  case GETCHILDREN_OP: {
    struct GetChildrenRequest GetChildReq;
    deserialize_GetChildrenRequest(ia, "GetChildReq", &GetChildReq);

    ZKLOG_INFO << " key " << GetChildReq.path << " watch " << GetChildReq.watch << std::endl;
    if(GetChildReq.watch)
      ZkReply::get_instance()->insert_watch(GetChildReq.path, conn);

    std::vector<std::string> sValue;
    uint64_t llVersion;
    phxkv::PhxKVStatus ret = m_oPhxKV.GetChild(GetChildReq.path, sValue, llVersion);
    int retCode = -1;
    if(ret == phxkv::PhxKVStatus::SUCC)
      retCode = 0;

    ZkReply::get_instance()->GetChildReply(conn, hdr.xid, sValue, retCode);
  }
  break;

  default:
    break;
  }

  close_buffer_iarchive(&ia);
}

void ZkServer::RemoveEphemeralZknode(const multipaxos::TCPConnPtr& conn){
  std::set<std::string> ephemeralNodes;
  ZkReply::get_instance()->GetRelativeEphemeral(conn, ephemeralNodes);
  for(std::set<std::string>::iterator it=ephemeralNodes.begin(); it!=ephemeralNodes.end();it++) {
    ZkReply::get_instance()->Remove_ephemeral_value(*it);
  }
  ZkReply::get_instance()->Remove_ephemeral_conn(conn);
}

ZkReply* ZkReply::m_replyhandle = new ZkReply();

ZkReply::ZkReply(){
  pthread_mutex_init(&lock, 0);
  pthread_cond_init(&cond, 0);
}

ZkReply* ZkReply::get_instance(){
  return m_replyhandle;
}

void ZkReply::insert_replay(const std::string& key, const multipaxos::TCPConnPtr& conn, const int32_t xid){
  ReplyParam rparam;

  if(m_rparam.find(key) != m_rparam.end()) {
    for(std::multimap<std::string, ReplyParam>::iterator it=m_rparam.begin(); it!=m_rparam.end(); it++) {
      if(it->second.conn == conn && it->second.xid == xid)
	return;
    }
  }

  rparam.conn = conn;
  rparam.xid = xid;
  m_rparam.insert(std::make_pair(key, rparam));

  //for(std::multimap<std::string, ReplyParam>::iterator it=m_rparam.begin(); it!=m_rparam.end(); it++)
  //	PLImp("request_reply m_rparam key: %s -- xid: %lu", key.c_str(), it->second.xid);
}


void ZkReply::insert_ephemeral_conn(const multipaxos::TCPConnPtr& conn, const std::string& key){
  std::set<std::string> exist_kvalue;
  if(m_ephermalnode.find(conn) != m_ephermalnode.end()) {
    exist_kvalue = m_ephermalnode[conn];
    if(exist_kvalue.find(key) == exist_kvalue.end()) {
      exist_kvalue.insert(key);
    }
  } else {
    exist_kvalue.insert(key);
  }

  m_ephermalnode[conn] = exist_kvalue;
}

void ZkReply::Remove_ephemeral_value(const std::string& skey){
  if(m_ephkvnodes.find(skey) != m_ephkvnodes.end()) {
    m_ephkvnodes.erase(skey);
  }
}

void ZkReply::Remove_ephemeral_conn(const multipaxos::TCPConnPtr& conn) {
  m_ephermalnode.erase(conn);
}

bool ZkReply::check_parent_ephemeralnode(const std::string& skey){
  std::string parentKey = skey.substr(0, skey.rfind("/"));
  if(parentKey.empty())
    return false;

  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    if(I->first == parentKey)
      return true;
  }

  return false;
}

KVClientRet ZkReply::Get_ephemeral_value(const std::string & sKey, std::string & sValue,
						uint64_t & llVersion) {
  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    if(I->first == sKey) {
      sValue = I->second.mValue;
      llVersion = I->second.mllVersion;
      return ZOK;
    }
  }
  return ZNONODE;
}

KVClientRet ZkReply::Set_ephemeral_value(const std::string & sKey, const std::string & sValue,
						const uint64_t llVersion){
  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    if(I->first == sKey) {
      if(llVersion != -1) {
	if(llVersion != I->second.mllVersion) {
	  ZKLOG_INFO << "Set key " << sKey << " version " << I->second.mllVersion << " is match, input version "
		     <<  llVersion;
	  return ZBADVERSION;
	} 
      }

      MemNodeParam memnode = I->second;
      memnode.mValue = sValue;
      memnode.mllVersion = llVersion;
      m_ephkvnodes[I->first] = memnode;
      return ZOK;
    }
  }
  return ZNONODE;
}

KVClientRet ZkReply::Create_ephemeral_value(const std::string & sKey, const std::string & sValue,
						   const uint64_t llVersion){
  MemNodeParam memnode;
  memnode.mValue = sValue;
  memnode.mllVersion = llVersion;

  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    if(I->first == sKey)
      return ZNODEEXISTS;
  }

  m_ephkvnodes.insert(std::make_pair(sKey, memnode));
  return ZOK;
}

KVClientRet ZkReply::Del_ephemeral_value(const std::string& sKey, const uint64_t llVersion){
  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    if(I->first == sKey) {
      if(llVersion != -1) {
	if(llVersion != I->second.mllVersion) {
	  ZKLOG_INFO << "Del key " << sKey << " version " << I->second.mllVersion << " is match, input version "
		     <<  llVersion;
	  return ZBADVERSION;
	} 
      }
    }
  }
  return ZNONODE;
}

KVClientRet ZkReply::GetChild_ephemeral_value(const std::string& sKey, std::vector<std::string>& sValue){
  for(std::map<std::string, MemNodeParam>::iterator I = m_ephkvnodes.begin(), E = m_ephkvnodes.end();
      I != E; I++) {
    std::string parent_key = I->first.substr(0, I->first.rfind("/"));
    if(parent_key == sKey)
      sValue.push_back(I->first);
  }
  return ZOK;
}

void ZkReply::insert_watch(const std::string& key, const multipaxos::TCPConnPtr& conn){
  if(m_rdatawatch.find(key) != m_rdatawatch.end()) {
    for(std::multimap<std::string, multipaxos::TCPConnPtr>::iterator it=m_rdatawatch.begin();
	it!=m_rdatawatch.end();it++) {
      if(it->second == conn)
	return;
    }
  }

  m_rdatawatch.insert(std::make_pair(key, conn));

  std::string parentKey = key.substr(0, key.rfind("/"));
  if(parentKey.empty())
    return;

  if(m_rdatawatch.find(parentKey) != m_rdatawatch.end()) {
    for(std::multimap<std::string, multipaxos::TCPConnPtr>::iterator it=m_rdatawatch.begin();
	it!=m_rdatawatch.end();it++) {
      if(it->second == conn)
	return;
    }
  }

  m_rdatawatch.insert(std::make_pair(parentKey, conn));
}

void ZkReply::request_reply(const std::string& key, const int32_t xid, const int type, const int RetCode){
  multipaxos::TCPConnPtr conn;
  int find_flag = 0;

  if(m_rparam.find(key) == m_rparam.end())
    return;

  for(std::multimap<std::string, ReplyParam>::iterator it=m_rparam.begin(); it!=m_rparam.end(); it++) {
    ZKLOG_INFO << "request_reply m_rparam key: " << key << " -- xid: " << it->second.xid;
    if(it->second.xid == xid) {
      conn = it->second.conn;
      m_rparam.erase(it);
      find_flag = 1;
    }
  }

  if(!find_flag)
    return;

  if(type == KVOperatorType_SET) {
    SetDataReply(conn, xid, type, RetCode);
  } else if(type == KVOperatorType_CREATE) {
    CreateDataReply(conn, key, xid, type, RetCode);
  } else if(type == KVOperatorType_DELETE) {
    DeleteDataReply(conn, xid, type, RetCode);
  } 
  return;
	
}

void ZkReply::watch_data_reply(const std::string& key, const int type){
  if(m_rdatawatch.find(key) == m_rdatawatch.end())
    return;

  for(std::multimap<std::string, multipaxos::TCPConnPtr>::iterator it=m_rdatawatch.begin();
      it!=m_rdatawatch.end(); ) {
    std::string skey = it->first;
    if(skey == key) {
      watch_reply_process(it->second, skey, type);
      m_rdatawatch.erase(it++);
    } else
      it++;
  }
}

void ZkReply::SetDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid,
			   const int type, const int RetCode){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=RetCode };
  oa = create_buffer_oarchive();
  serialize_ReplyHeader(oa, "header", &h);

  send_msg_client(conn, oa);
  notify_completion();
}

void ZkReply::GetRelativeEphemeral(const multipaxos::TCPConnPtr& conn, std::set<std::string>& ephemeralNodes){
  if(m_ephermalnode.find(conn) != m_ephermalnode.end()) {
    ephemeralNodes = m_ephermalnode[conn];
  }
}

void ZkReply::CreateDataReply(const multipaxos::TCPConnPtr& conn, const std::string& key,
			      const int32_t xid, const int type, const int RetCode){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=RetCode };
  oa = create_buffer_oarchive();
  serialize_ReplyHeader(oa, "header", &h);

  if(RetCode == 0) {
    struct CreateResponse CreateResp = {.path = (char*)key.c_str()};
    serialize_CreateResponse(oa, "CreateResp", &CreateResp);
  }

  send_msg_client(conn, oa);
  notify_completion();
}

void ZkReply::DeleteDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid,
			      const int type, const int RetCode){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=RetCode };
  oa = create_buffer_oarchive();
  serialize_ReplyHeader(oa, "header", &h);

  send_msg_client(conn, oa);
  notify_completion();
}

void ZkReply::GetDataReply(const multipaxos::TCPConnPtr& conn, const int32_t xid,
			   const std::string& sValue, int retCode){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=retCode };
  oa = create_buffer_oarchive();
  int rc = serialize_ReplyHeader(oa, "header", &h);
  if(retCode == 0) {
    struct GetDataResponse dataResp;
    ardb::Buffer dataBuf;
    dataBuf.Write(sValue.c_str(), sValue.size());
    dataResp.data.len = (int32_t)sValue.size();
    dataResp.data.buff = (char*)dataBuf.c_str();

    struct Stat stat_ = {.czxid = g_zxid-1, .mzxid=g_zxid-1, .ctime=time(NULL)*1000,
			 .mtime=time(NULL)*1000, .version=0, .cversion=0, .aversion=0,
			 .ephemeralOwner=0, .dataLength=(int32_t)sValue.size(),.numChildren=0, .pzxid=g_zxid-1 };
    dataResp.stat = stat_;

    rc = rc < 0 ? rc : serialize_GetDataResponse(oa, "ReplyGetData", &dataResp);
  }

  send_msg_client(conn, oa);
}

void ZkReply::GetChildReply(const multipaxos::TCPConnPtr& conn, const int32_t xid,
			    const std::vector<std::string>& sValue, int retCode){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=retCode };
  oa = create_buffer_oarchive();
  int rc = serialize_ReplyHeader(oa, "header", &h);

  struct GetChildrenResponse GetChildResp;
  allocate_String_vector(&GetChildResp.children,sValue.size());
  for(int i=0;i<(int)sValue.size();++i)
    GetChildResp.children.data[i]=strdup(sValue[i].c_str());
  
  serialize_GetChildrenResponse(oa, "GetChildResp", &GetChildResp);
  deallocate_GetChildrenResponse(&GetChildResp);
  send_msg_client(conn, oa);
}

void ZkReply::ExistReply(const multipaxos::TCPConnPtr& conn, const int32_t xid, int retCode, int32_t version){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=retCode };
  oa = create_buffer_oarchive();
  int rc = serialize_ReplyHeader(oa, "header", &h);

  struct Stat stat_ = {.czxid = g_zxid-1, .mzxid=g_zxid-1, .ctime=time(NULL)*1000,
		       .mtime=time(NULL)*1000, .version=0, .cversion=version, .aversion=0,
		       .ephemeralOwner=0, .dataLength=0, .numChildren=1, .pzxid=g_zxid-1 };
  struct ExistsResponse ExistsResp;
  ExistsResp.stat = stat_;
  rc = rc < 0 ? rc : serialize_ExistsResponse(oa, "ReplyExist", &ExistsResp);

  send_msg_client(conn, oa);
}

void ZkReply::PingReply(const multipaxos::TCPConnPtr& conn, const int32_t xid){
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = xid, .zxid=g_zxid, .err=0 };
  oa = create_buffer_oarchive();
  int rc = serialize_ReplyHeader(oa, "header", &h);
  send_msg_client(conn, oa);
}

void ZkReply::watch_reply_process(const multipaxos::TCPConnPtr& conn, const std::string& key, const int type){
  ZKLOG_INFO << "watch_reply_process key: " << key << " type: " << type;
  struct oarchive *oa;
  struct ReplyHeader h = {.xid = WATCHER_EVENT_XID, .zxid=-1, .err=0 };
  oa = create_buffer_oarchive();
  int rc = serialize_ReplyHeader(oa, "header", &h);
  ardb::Buffer dataBuf;
  dataBuf.Write(key.c_str(), key.size());

  struct WatcherEvent Watchevt;
  Watchevt.path = (char*)dataBuf.c_str();
  //key.copy(Watchevt.path, key.size());
  Watchevt.type = type;
  Watchevt.state = CONNECTED_STATE_DEF;

  rc = rc < 0 ? rc : serialize_WatcherEvent(oa, "Watchevt", &Watchevt);
  send_msg_client(conn, oa);
}

void ZkReply::send_msg_client(const multipaxos::TCPConnPtr& conn, struct oarchive *oa){
  ardb::Buffer sendBuf;

  int buffer_len = htonl(get_buffer_len(oa));
  sendBuf.Write(&buffer_len, 4);
  sendBuf.Write(get_buffer(oa), get_buffer_len(oa));

  write(conn->fd(), sendBuf.c_str(), get_buffer_len(oa)+4);
  close_buffer_oarchive(&oa, get_buffer_len(oa));
}

void ZkReply::wait_completion(){
  pthread_mutex_lock(&lock);
  pthread_cond_wait(&cond, &lock);
  pthread_mutex_unlock(&lock);
}

void ZkReply::notify_completion(){
  pthread_mutex_lock(&lock);
  pthread_cond_broadcast(&cond);
  pthread_mutex_unlock(&lock);
}
