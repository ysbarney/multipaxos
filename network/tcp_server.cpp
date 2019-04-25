#include "inner_pre.h"

#include "tcp_server.h"
#include "listener.h"
#include "tcp_conn.h"
#include "libevent.h"

namespace multipaxos {
TCPServer::TCPServer(EventLoop* loop,
                     const std::string& laddr,
                     const std::string& name,
                     uint32_t thread_num)
    : loop_(loop)
    , listen_addr_(laddr)
    , name_(name)
    , conn_fn_(&internal::DefaultConnectionCallback)
    , msg_fn_(&internal::DefaultMessageCallback)
    , next_conn_id_(0) {
    PAXOSLOG_TRACE << "name=" << name << " listening addr " << laddr << " thread_num=" << thread_num;
    tpool_.reset(new EventLoopThreadPool(loop_, thread_num));
}

TCPServer::~TCPServer() {
    //DLOG_TRACE;
    assert(connections_.empty());
    assert(!listener_);
    if (tpool_) {
        assert(tpool_->IsStopped());
        tpool_.reset();
    }
}

bool TCPServer::Init() {
    //DLOG_TRACE;
    assert(status_ == kNull);
    listener_.reset(new Listener(loop_, listen_addr_));
    listener_->Listen();
    status_.store(kInitialized);
    return true;
}

void TCPServer::AfterFork() {
    tpool_->AfterFork();
}

bool TCPServer::Start() {
    //DLOG_TRACE;
    assert(status_ == kInitialized);
    status_.store(kStarting);
    assert(listener_.get());
    bool rc = tpool_->Start(true);
    if (rc) {
        assert(tpool_->IsRunning());
        listener_->SetNewConnectionCallback(
            std::bind(&TCPServer::HandleNewConn,
                      this,
                      std::placeholders::_1,
                      std::placeholders::_2,
                      std::placeholders::_3));

        // We must set status_ to kRunning firstly and then we can accept new
        // connections. If we use the following code :
        //     listener_->Accept();
        //     status_.store(kRunning);
        // there is a chance : we have accepted a connection but status_ is not
        // kRunning that will cause the assert(status_ == kRuning) failed in
        // TCPServer::HandleNewConn.
        status_.store(kRunning);
        listener_->Accept();
    }
    return rc;
}

void TCPServer::Stop(DoneCallback on_stopped_cb) {
    PAXOSLOG_TRACE << "Entering ...";
    assert(status_ == kRunning);
    status_.store(kStopping);
    substatus_.store(kStoppingListener);
    loop_->RunInLoop(std::bind(&TCPServer::StopInLoop, this, on_stopped_cb));
}

void TCPServer::StopInLoop(DoneCallback on_stopped_cb) {
    PAXOSLOG_TRACE << "Entering ...";
    assert(loop_->IsInLoopThread());
    listener_->Stop();
    listener_.reset();

    if (connections_.empty()) {
        // Stop all the working threads now.
        PAXOSLOG_TRACE << "no connections";
        StopThreadPool();
        if (on_stopped_cb) {
            on_stopped_cb();
            on_stopped_cb = DoneCallback();
        }
        status_.store(kStopped);
    } else {
        PAXOSLOG_TRACE << "close connections";
        for (auto& c : connections_) {
            if (c.second->IsConnected()) {
                PAXOSLOG_TRACE << "close connection id=" << c.second->id() << " fd=" << c.second->fd();
                c.second->Close();
            } else {
                PAXOSLOG_TRACE << "Do not need to call Close for this TCPConn it may be doing disconnecting. TCPConn=" << c.second.get() << " fd=" << c.second->fd() << " status=" << StatusToString();
            }
        }

        stopped_cb_ = on_stopped_cb;

        // The working threads will be stopped after all the connections closed.
    }

    PAXOSLOG_TRACE << "exited, status=" << StatusToString();
}

void TCPServer::StopThreadPool() {
    PAXOSLOG_TRACE << "pool=" << tpool_.get();
    assert(loop_->IsInLoopThread());
    assert(IsStopping());
    substatus_.store(kStoppingThreadPool);
    tpool_->Stop(true);
    assert(tpool_->IsStopped());

    // Make sure all the working threads totally stopped.
    tpool_->Join();
    tpool_.reset();

    substatus_.store(kSubStatusNull);
}

void TCPServer::HandleNewConn(multipaxos_socket_t sockfd,
                              const std::string& remote_addr/*ip:port*/,
                              const struct sockaddr_in* raddr) {
    PAXOSLOG_TRACE << "fd=" << sockfd;
    assert(loop_->IsInLoopThread());
    if (IsStopping()) {
        PAXOSLOG_WARN << "this=" << this << " The server is at stopping status. Discard this socket fd=" << sockfd << " remote_addr=" << remote_addr;
        EVUTIL_CLOSESOCKET(sockfd);
        return;
    }

    assert(IsRunning());
    EventLoop* io_loop = GetNextLoop(raddr);
#ifdef H_DEBUG_MODE
    std::string n = name_ + "-" + remote_addr + "#" + std::to_string(next_conn_id_);
#else
    std::string n = remote_addr;
#endif
    ++next_conn_id_;
    TCPConnPtr conn(new TCPConn(io_loop, n, sockfd, listen_addr_, remote_addr, next_conn_id_));
    assert(conn->type() == TCPConn::kIncoming);
    conn->SetMessageCallback(msg_fn_);
    conn->SetConnectionCallback(conn_fn_);
    conn->SetCloseCallback(std::bind(&TCPServer::RemoveConnection, this, std::placeholders::_1));
    io_loop->RunInLoop(std::bind(&TCPConn::OnAttachedToLoop, conn));
    connections_[conn->id()] = conn;
}

EventLoop* TCPServer::GetNextLoop(const struct sockaddr_in* raddr) {
    if (IsRoundRobin()) {
        return tpool_->GetNextLoop();
    } else {
        return tpool_->GetNextLoopWithHash(raddr->sin_addr.s_addr);
    }
}

void TCPServer::RemoveConnection(const TCPConnPtr& conn) {
    PAXOSLOG_TRACE << "conn=" << conn.get() << " fd="<< conn->fd() << " connections_.size()=" << connections_.size();
    auto f = [this, conn]() {
        // Remove the connection in the listening EventLoop
        PAXOSLOG_TRACE << "conn=" << conn.get() << " fd="<< conn->fd() << " connections_.size()=" << connections_.size();
        assert(this->loop_->IsInLoopThread());
        this->connections_.erase(conn->id());
        if (IsStopping() && this->connections_.empty()) {
            // At last, we stop all the working threads
            PAXOSLOG_TRACE << "stop thread pool";
            assert(substatus_.load() == kStoppingListener);
            StopThreadPool();
            if (stopped_cb_) {
                stopped_cb_();
                stopped_cb_ = DoneCallback();
            }
            status_.store(kStopped);
        }
    };
    loop_->RunInLoop(f);
}

}

