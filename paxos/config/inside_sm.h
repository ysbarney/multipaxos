/*
 * Module_name: inside_sm.h
 * Author: Barneyliu
 * Time: 2019-01-07
 * Description:
 *
 */

#pragma once

#include "sm.h"
#include <string>

namespace multipaxos
{

class InsideSM : public StateMachine
{
public:
    virtual ~InsideSM() {}

    virtual int GetCheckpointBuffer(std::string & sCPBuffer) = 0;

    virtual int UpdateByCheckpoint(const std::string & sCPBuffer, bool & bChange) = 0;
};
    
}

