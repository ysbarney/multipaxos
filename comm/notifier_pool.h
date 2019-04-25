/*
 * Module_name: notifier_pool.h
 * Author: Barneyliu
 * Time: 2019-01-15
 * Description:
 *
 */

#pragma once

#include <map>
#include <mutex>

namespace multipaxos
{

class Notifier
{
public:
    Notifier();
    ~Notifier();

    int Init();

    void SendNotify(const int ret);

    void WaitNotify(int & ret);

private:
    int m_iPipeFD[2];
};

/////////////////////////////////

class NotifierPool
{
public:
    NotifierPool();
    ~NotifierPool();

    int GetNotifier(const uint64_t iID, Notifier *& poNotifier);

private:
    std::map<uint64_t, Notifier *> m_mapPool;
    std::mutex m_oMutex;
};

}

