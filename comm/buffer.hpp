/*
 *Copyright (c) 2013-2013, yinqiwen <yinqiwen@gmail.com>
 *All rights reserved.
 *
 *Redistribution and use in source and binary forms, with or without
 *modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 *THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 *AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 *IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 *ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 *BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 *CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 *SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 *INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 *CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 *ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 *THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef BUFFER__HPP_
#define BUFFER__HPP_
#include <sys/types.h>
#include <stdarg.h>
#include <stdlib.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <string>

namespace ardb
{
/**
 * A dynamic buffer for read/write.
 *
 *       +-------------------+------------------+------------------+
 *       | readed bytes      |  readable bytes  |  writable bytes  |
 *       +-------------------+------------------+------------------+
 *       |                   |                  |                  |
 *       0      <=      readerIndex   <=   writerIndex    <=    capacity
 *
 */
class Buffer {
private:
    char m_firstBuff[4096];

    char *m_buffer; //raw buffer
    /** total allocation available in the buffer field. */
    size_t m_buffer_len; //raw buffer length

    size_t m_write_idx;
    size_t m_read_idx;
    bool m_in_heap;
public:
    static const int BUFFER_MAX_READ = 8192;
    static const size_t DEFAULT_BUFFER_SIZE = 4096;
    inline Buffer () :
                    m_buffer (m_firstBuff), m_buffer_len (sizeof(m_firstBuff)), m_write_idx (0), m_read_idx (0), m_in_heap (false) {

    }
    inline Buffer ( char* value , int off , int len ) :
                    m_buffer (value), m_buffer_len (len), m_write_idx (len), m_read_idx (off), m_in_heap (false) {

    }
//    inline Buffer ( size_t size ) :
//                    m_buffer (0), m_buffer_len (0), m_write_idx (0), m_read_idx (0), m_in_heap (true) {
//        EnsureWritableBytes (size);
//    }
    inline size_t GetReadIndex () {
        return m_read_idx;
    }
    inline size_t GetWriteIndex () {
        return m_write_idx;
    }
    inline void SetReadIndex ( size_t idx ) {
        m_read_idx = idx;
    }
    inline void AdvanceReadIndex ( int step ) {
        m_read_idx += step;
    }
    inline void SetWriteIndex ( size_t idx ) {
        m_write_idx = idx;
    }
    inline void AdvanceWriteIndex ( int step ) {
        m_write_idx += step;
    }
    inline bool Readable () const {
        return m_write_idx > m_read_idx;
    }
    inline bool Writeable () const {
//        return m_buffer_len > m_write_idx;
        return m_write_idx < m_buffer_len;
    }
    inline size_t ReadableBytes () const {
        return Readable () ? m_write_idx - m_read_idx : 0;
    }
    inline size_t WriteableBytes () const {
        return Writeable () ? m_buffer_len - m_write_idx : 0;
    }

    //至少保留leastLength可写，调用之前一般先调用DiscardReadedBytes
    //返回缩减多少字符
    //暂时不用，以后再调整
//    inline size_t Compact ( size_t leastLength ) {
//        if (!Readable ()) {
//            Clear ();
//        }
//        uint32_t writableBytes = WriteableBytes ();
//        if (writableBytes < leastLength) {
//            return 0;
//        }
//        uint32_t readableBytes = ReadableBytes ();
//        uint32_t total = Capacity ();
//
//        uint32_t newTotal = Capacity () / 2;    //直接针对原始长度缩减1/2
//        if (newTotal <= readableBytes + leastLength)    //不能缩减，因为长度不够
//                        {
//            return 0;
//        }
//
//        if (newTotal <= 1024 * 1024)    //最低保持1M，避免频繁分配
//                        {
//            return 0;
//        }
//
//        char* newSpace = NULL;
//
//        newSpace = (char*) malloc (newTotal);
//        if (NULL == newSpace) {
//            return 0;
//        }
//        memcpy (newSpace , m_buffer + m_read_idx , readableBytes);
//
//        if (NULL != m_buffer && m_in_heap) {
//            free (m_buffer);
//        }
//        m_read_idx = 0;
//        m_write_idx = readableBytes;
//        m_buffer_len = newTotal;
//        m_buffer = newSpace;
//        m_in_heap = true;
//        printf ("Compact least:%zd,m_write_idex:%zd,old len:%u,new len:%zd\n" , leastLength , m_write_idx , total , m_buffer_len);
//        return total - newTotal;
//    }

    bool EnsureWritableBytes ( size_t minWritableBytes , bool growzero = false );

    inline bool Reserve ( size_t len ) {
        return EnsureWritableBytes (len);
    }
    inline const char* GetRawBuffer () const {
        return m_buffer;
    }
    inline const char* GetRawWriteBuffer () const {
        return m_buffer + m_write_idx;
    }
    inline const char* GetRawReadBuffer () const {
        return m_buffer + m_read_idx;
    }
    inline size_t Capacity () const {
        return m_buffer_len;
    }
//    inline void Limit () {
//        m_buffer_len = m_write_idx;
//    }
    inline void Clear () {
        m_write_idx = m_read_idx = 0;
    }
    inline int Read ( void *data_out , size_t datlen ) {
        if (datlen > ReadableBytes ()) {
            return -1;
        }
        memcpy (data_out , m_buffer + m_read_idx , datlen);
        m_read_idx += datlen;
        return datlen;
    }
    inline int Write ( const void *data_in , size_t datlen ) {
        if (!EnsureWritableBytes (datlen)) {
            return -1;
        }
        memcpy (m_buffer + m_write_idx , data_in , datlen);
        m_write_idx += datlen;
        return datlen;
    }
    //读取unit的可读部分，并写入本buffer
    inline int Write ( Buffer* unit , size_t datlen ) {
        if (NULL == unit) {
            return -1;
        }
        if (datlen > unit->ReadableBytes ()) {
            datlen = unit->ReadableBytes ();
        }
        int ret = Write (unit->m_buffer + unit->m_read_idx , datlen);
        if (ret > 0) {
            unit->m_read_idx += ret;
        }
        return ret;
    }

    inline int WriteByte ( char ch ) {
        return Write (&ch , 1);
    }

    inline int Read ( Buffer* unit , size_t datlen ) {
        if (NULL == unit) {
            return -1;
        }
        return unit->Write (this , datlen);
    }

    inline int SetBytes ( void* data , size_t datlen , size_t index ) {
        if (NULL == data) {
            return -1;
        }
        if (index + datlen > m_write_idx) {
            return -1;
        }
        memcpy (m_buffer + index , data , datlen);
        return datlen;
    }

    inline bool ReadByte ( char& ch ) {
        return Read (&ch , 1) == 1;
    }

    inline int Copyout ( void *data_out , size_t datlen ) {
        if (datlen == 0) {
            return 0;
        }
        if (datlen > ReadableBytes ()) {
            datlen = ReadableBytes ();
        }
        memcpy (data_out , m_buffer + m_read_idx , datlen);
        return datlen;
    }

    //读偏移增加len
    inline void SkipBytes ( size_t len ) {
        AdvanceReadIndex (len);
    }
    //读写缓冲区迁移
    inline void shiftBuffer () {
        if (m_read_idx > 0) {
            if (Readable ()) {
                size_t tmp = ReadableBytes ();
                memmove (m_buffer , m_buffer + m_read_idx , tmp);
                m_read_idx = 0;
                m_write_idx = tmp;
            }
            else {
                m_read_idx = m_write_idx = 0;
            }
        }
    }
    //在buf中的m_buffer[start,end)中寻找第一个与data匹配的指针,注意如果计算真正内容的偏移的话需要减去m_read_idx
    int IndexOf ( const void* data , size_t len , size_t start , size_t end );
    int IndexOf ( const void* data , size_t len );
    int Printf ( const char *fmt , ... );
    int PrintBinary ( const void * ptr , int len );
//    static int tranBufToReadbleBuffer ( const void * ptr , int len , char * dstBuf , int dstLen );//静态函数，将ptr转化成可读的字符串输出
    int VPrintf ( const char *fmt , va_list ap );
    int ReadFD ( int fd , int& err );
    int WriteFD ( int fd , int& err );

    inline std::string AsString () {
        if (m_buffer) {
            return std::string (m_buffer + m_read_idx , ReadableBytes ());
        }
        else {
            return "";
        }

    }

    void releaseseBuf ();

    ~Buffer ();

    const char * c_str () const {
        if (m_buffer) {
            if (Writeable ()) {
                m_buffer[m_write_idx] = 0;
            }
            return m_buffer + m_read_idx;
        }
        else {
            return "";
        }

    }

    size_t length () const {
        return ReadableBytes ();
    }

private:
    Buffer ( const Buffer & );
    Buffer & operator= ( const Buffer & );

};
}

#endif /* BUFFER__HPP_ */
