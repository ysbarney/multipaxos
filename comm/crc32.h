/*
 * Module_name: crc32.h
 * Author: Barneyliu
 * Time: 2018-12-21
 * Description:
 *
 */

#ifndef __CRC32_H__
#define __CRC32_H__

#include <stdint.h>

#define CRC32SKIP 8
#define NET_CRC32SKIP 7 

uint32_t crc32(uint32_t crc, const uint8_t *buf, int len, int skiplen = 1);

#endif

