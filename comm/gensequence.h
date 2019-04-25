/*
 * Module_name: gensequence.h
 * Author: Barneyliu
 * Time: 2019-01-07
 * Description:
 *
 */

#include <stdint.h>

namespace multipaxos 
{

class gensequence
{
public:
	gensequence() : m_llinstance(0)
	{}
	virtual ~gensequence()
	{}

	uint64_t GenInstanceID();
	uint64_t GetInstanceID();
	void SetInstanceID(const uint64_t llInstanceID);
	
private:
	uint64_t m_llinstance;
};

}
