/*
 * Module_name: concurrent.cpp
 * Author: Barneyliu
 * Time: 2018-12-19
 * Description:
 *
 */

#include "concurrent.h"
#include <functional>
#include <iostream>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static void* mmThreadRun(void* p) {
    multipaxos::Thread* thread = (multipaxos::Thread*)p;
    thread->run();
    return 0;
}

namespace multipaxos {

///////////////////////////////////////////////////////////Thread

Thread::Thread() {}

Thread::~Thread() {}

void Thread::start() {
    _thread = std::thread(std::bind(&mmThreadRun, this));
}

void Thread::join() {
    _thread.join();
}

void Thread::detach() {
    _thread.detach();
}
    
std::thread::id Thread::getId() const {
    return _thread.get_id();
}

void Thread::sleep(int ms) {
    std::this_thread::sleep_for(std::chrono::milliseconds(ms));
}

} 


