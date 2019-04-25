/*
 * Module_name: serial_lock.h
 * Author: Barneyliu
 * Time: 2019-01-22
 * Description:
 *
 */

#pragma once

#include <mutex>
#include <condition_variable>

namespace multipaxos
{

class SerialLock
{
public:
    SerialLock();
    ~SerialLock();

    void Lock();
    void UnLock();

    void Wait();
    void Interupt();

    bool WaitTime(const int iTimeMs);

private:
    std::mutex m_oMutex;
    std::condition_variable_any m_oCond;
};

}

