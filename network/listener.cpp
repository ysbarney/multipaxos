#include "inner_pre.h"

#include "listener.h"
#include "event_loop.h"
#include "fd_channel.h"
#include "libevent.h"
#include "sockets.h"

namespace multipaxos {
Listener::Listener(EventLoop* l, const std::string& addr)
    : loop_(l), addr_(addr) {
    PAXOSLOG_TRACE << "addr=" << addr;
}

Listener::~Listener() {
    PAXOSLOG_TRACE << "fd=" << chan_->fd();
    chan_.reset();
    EVUTIL_CLOSESOCKET(fd_);
    fd_ = INVALID_SOCKET;
}

void Listener::Listen(int backlog) {
    //DLOG_TRACE;
    fd_ = sock::CreateNonblockingSocket();
    if (fd_ < 0) {
        int serrno = errno;
        PAXOSLOG_FATAL << "Create a nonblocking socket failed " << strerror(serrno);
        return;
    }

    struct sockaddr_storage addr = sock::ParseFromIPPort(addr_.data());
    // TODO Add retry when failed
    int ret = ::bind(fd_, sock::sockaddr_cast(&addr), static_cast<socklen_t>(sizeof(struct sockaddr)));
    if (ret < 0) {
        int serrno = errno;
        PAXOSLOG_FATAL << "bind error :" << strerror(serrno) << " . addr=" << addr_;
    }

    ret = ::listen(fd_, backlog);
    if (ret < 0) {
        int serrno = errno;
        PAXOSLOG_FATAL << "Listen failed " << strerror(serrno);
    }
}

void Listener::Accept() {
    //DLOG_TRACE;
    chan_.reset(new FdChannel(loop_, fd_, true, false));
    chan_->SetReadCallback(std::bind(&Listener::HandleAccept, this));
    loop_->RunInLoop(std::bind(&FdChannel::AttachToLoop, chan_.get()));
    PAXOSLOG_INFO << "TCPServer is running at " << addr_;
}

void Listener::HandleAccept() {
    PAXOSLOG_TRACE << "A new connection is comming in";
    assert(loop_->IsInLoopThread());
    struct sockaddr_storage ss;
    socklen_t addrlen = sizeof(ss);
    int nfd = -1;
    if ((nfd = ::accept(fd_, sock::sockaddr_cast(&ss), &addrlen)) == -1) {
        int serrno = errno;
        if (serrno != EAGAIN && serrno != EINTR) {
            PAXOSLOG_WARN << __FUNCTION__ << " bad accept " << strerror(serrno);
        }
        return;
    }

    if (evutil_make_socket_nonblocking(nfd) < 0) {
        PAXOSLOG_ERROR << "set fd=" << nfd << " nonblocking failed.";
        EVUTIL_CLOSESOCKET(nfd);
        return;
    }

    sock::SetKeepAlive(nfd, true);

    std::string raddr = sock::ToIPPort(&ss);
    if (raddr.empty()) {
        PAXOSLOG_ERROR << "sock::ToIPPort(&ss) failed.";
        EVUTIL_CLOSESOCKET(nfd);
        return;
    }

    PAXOSLOG_TRACE << "accepted a connection from " << raddr
        << ", listen fd=" << fd_
        << ", client fd=" << nfd;

    if (new_conn_fn_) {
        new_conn_fn_(nfd, raddr, sock::sockaddr_in_cast(&ss));
    }
}

void Listener::Stop() {
    assert(loop_->IsInLoopThread());
    chan_->DisableAllEvent();
    chan_->Close();
}
}

