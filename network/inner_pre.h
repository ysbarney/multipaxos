#pragma once

#include <assert.h>
#include <stdint.h>

#ifdef __cplusplus
#include <iostream>
#include <memory>
#include <functional>
#endif // end of define __cplusplus

#include "platform_config.h"
#include "sys_addrinfo.h"
#include "sys_sockets.h"
#include "sockets.h"
#include "async_boost_log.h"

struct event;
namespace multipaxos {
    int EventAdd(struct event* ev, const struct timeval* timeout);
    int EventDel(struct event*);
    MULTIPAXOS_EXPORT int GetActiveEventCount();
}

