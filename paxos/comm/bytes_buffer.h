/*
 * Module_name: bytes_buffer.h
 * Author: Barneyliu
 * Time: 2019-01-22
 * Description:
 *
 */

#pragma once

namespace multipaxos
{

class BytesBuffer
{
public:
    BytesBuffer();
    ~BytesBuffer();

    char * GetPtr();

    int GetLen();

    void Ready(const int iBufferLen);

private:
    char * m_pcBuffer;
    int m_iLen;
};
    
}

