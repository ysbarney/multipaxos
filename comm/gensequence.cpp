/*
 * Module_name: gensequence.cpp
 * Author: Barneyliu
 * Time: 2019-01-07
 * Description:
 *
 */
 
#include "gensequence.h"

namespace multipaxos 
{

uint64_t gensequence::GenInstanceID()
{
	return ++m_llinstance;
}

uint64_t gensequence::GetInstanceID() 
{
	return m_llinstance;
}

void gensequence::SetInstanceID(const uint64_t llInstanceID)
{
	m_llinstance = llInstanceID;
}

}