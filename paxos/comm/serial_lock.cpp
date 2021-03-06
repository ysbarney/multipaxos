/*
 * Module_name: serial_lock.cpp
 * Author: Barneyliu
 * Time: 2019-01-22
 * Description:
 *
 */

#include "serial_lock.h"
#include "util.h"

namespace multipaxos
{

SerialLock :: SerialLock()
{
}

SerialLock :: ~SerialLock()
{
}

void SerialLock :: Lock()
{
    m_oMutex.lock();
}

void SerialLock :: UnLock()
{
    m_oMutex.unlock();
}

void SerialLock :: Wait()
{
    m_oCond.wait(m_oMutex);
}

void SerialLock :: Interupt()
{
    m_oCond.notify_one();
}

bool SerialLock :: WaitTime(const int iTimeMs)
{
    return m_oCond.wait_for(m_oMutex, std::chrono::milliseconds(iTimeMs)) != std::cv_status::timeout;
}

}


