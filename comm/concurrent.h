/*
 * Module_name: concurrent.h
 * Author: Barneyliu
 * Time: 2018-12-19
 * Description:
 *
 */

#pragma once

#include <boost/thread/thread.hpp>
#include <boost/lockfree/queue.hpp>

#include "util.h"
#include <deque>
#include <pthread.h>
#include <signal.h>
#include <sched.h>
#include <cassert>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <queue>
#include <list>
#include <map>
#include <iostream>
#include <string>
#include <condition_variable>
#include <mutex>
#include <thread>

#include "concurrentqueue.h"

//#include "ConcurrentMap_Leapfrog.h"

namespace multipaxos {

using std::deque;

class Thread : public Noncopyable {
public:
    Thread();

    virtual ~Thread();

    void start();

    void join();

    void detach();
    
    std::thread::id getId() const;

    virtual void run() = 0;

    static void sleep(int ms);

protected:
    std::thread _thread;
};

template <class T>
class Queue {
public:
    Queue() : _size(0) {}

    virtual ~Queue() {}

    T& peek() {
        while (empty()) {
            _cond.wait(_lock);
        }
        return _storage.front();
    }

    size_t peek(T& value) {
        while (empty()) {
            _cond.wait(_lock);
        }
        value = _storage.front();
        return _size;
    }

    bool peek(T& t, int timeoutMS) {
        while (empty()) {
            if (_cond.wait_for(_lock, std::chrono::milliseconds(timeoutMS)) == std::cv_status::timeout) {
                return false;
            }
        }
        t = _storage.front();
        return true;
    }

    size_t pop(T* values, size_t n) {
        while (empty()) {
            _cond.wait(_lock);
        }

        size_t i = 0;
        while (!_storage.empty() && i < n) {
            values[i] = _storage.front();
            _storage.pop_front();
            --_size;
            ++i;
        }

        return i;
    }

    size_t pop() {
        _storage.pop_front();
        return --_size;
    }

    virtual size_t add(const T& t, bool signal = true, bool back = true) {
        if (back) {
            _storage.push_back(t);
        } else {
            _storage.push_front(t);
        }

        if (signal) {
            _cond.notify_one();
        }

        return ++_size;
    }

    bool empty() const {
        return _storage.empty();
    }

    size_t size() const {
        return _storage.size();
    }

    void clear() {
        _storage.clear();
    }

    void signal() {
        _cond.notify_one();
    }

    void broadcast() {
        _cond.notify_all();
    }

    virtual void lock() {
        _lock.lock();
    }

    virtual void unlock() {
        _lock.unlock();
    }

    void swap(Queue& q) {
        _storage.swap( q._storage );
        int size = q._size;
        q._size = _size;
        _size = size;
    }

protected:
    std::mutex _lock;
    std::condition_variable_any _cond;
    deque<T> _storage;
    size_t _size;
};

template<class T>
class boost_queue {
public:
	boost_queue() : _size(0)
	{
	}

	virtual ~boost_queue() {}

	bool peek(T& value) {
        while (empty()) {
            _cond.wait(_lock);
        }
		
        bool ret = _storage.pop(value);
		if(ret) {
			--_size;
		}
		return ret;
    }

	bool peek(T& value, int timeoutMS) {
		while (empty()) {
            if (_cond.wait_for(_lock, std::chrono::milliseconds(timeoutMS)) == std::cv_status::timeout) {
                return false;
            }
        }
		
		bool ret = _storage.pop(value);
		if(ret) {
			--_size;
		}
		return ret;
	}

	virtual size_t add(const T& t, bool signal = true) {
        _storage.push(t);

		if (signal) {
            _cond.notify_one();
        }
        return ++_size;
    }

	size_t pop() {
		T value;
        bool ret = _storage.pop(value);
		if(ret)
			--_size;
		
        return _size;
    }

	bool empty() 
	{
		return (_storage.empty());
	}
	
	size_t size() const {
        return _size;
    }
	
protected:
	std::mutex _lock;
	std::condition_variable_any _cond;
    boost::lockfree::queue<T> _storage;
    size_t _size;
};

struct Traits : public moodycamel::ConcurrentQueueDefaultTraits
{
	// Use a slightly larger default block size; the default offers
	// a good trade off between speed and memory usage, but a bigger
	// block size will improve throughput (which is mostly what
	// we're after with these benchmarks).
	static const size_t BLOCK_SIZE = 64;
};

template<class T>
class concurrent_queue {
public:
	concurrent_queue() : _size(0) {
		
	}
	virtual ~concurrent_queue() {}

	bool peek(T& value) {
        if (empty()) {
        	return false;
        }
		
        bool ret = _storage.try_dequeue(value);
		if(ret) {
			--_size;
		}
		return ret;
    }
	
	bool peek(T& value, int timeoutMS) {
		while (empty()) {
            if (_cond.wait_for(_lock, std::chrono::milliseconds(timeoutMS)) == std::cv_status::timeout) {
                return false;
            }
        }
		
		bool ret = _storage.try_dequeue(value);
		if(ret) {
			--_size;
		}
		return ret;
	}
	
	virtual size_t add(const T& t, bool signal = true) {
        _storage.enqueue(t);

		if (signal) {
            _cond.notify_one();
        }
        return ++_size;
    }

	size_t pop() {
		T value;
        bool ret = _storage.try_dequeue(value);
		if(ret)
			--_size;
		
        return _size;
    }

	bool empty() 
	{
		return (_storage.size_approx() == 0);
	}
	
	size_t size() const {
        return _storage.size_approx();
    }
private:
	std::mutex _lock;
	std::condition_variable_any _cond;
	moodycamel::ConcurrentQueue<T, Traits> _storage;
	size_t _size;
};


class SyncException : public SysCallException {
public:
    SyncException(int errCode, const string& errMsg, bool detail = true)
        : SysCallException(errCode, errMsg, detail) {}

    virtual ~SyncException() throw () {}
};

class ThreadException : public SysCallException {
public:
    ThreadException(const string& errMsg, bool detail = true)
        : SysCallException(errno, errMsg, detail) {}

    virtual ~ThreadException() throw () {}
};

#if 0
template<typename T>
class asyn_queue_worker : public Thread , public tbb_queue
{
public:
	asyn_queue_worker();
	virtual ~asyn_queue_worker();

	virtual int Init() = 0;
	virtual int OnReceiveMsg(const T pcMsg, const int iMsgLen) = 0;

private:
	tbb_queue<T> m_omsgQueue;
};
#endif

} 

