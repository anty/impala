/*
 * hfile-types.cc
 *
 *  Created on: 2013-5-20
 *      Author: Rao
 */

#include "hfile-types.h"
#include "hfile-common.h"
#include "exec/read-write-util.h"
#include "common/status.h"

#include <iostream>
#include <sstream>
#include <string>
#include <memory>

using namespace impala;
using namespace std;

namespace hfile
{
static	const uint32_t MIN_FORMAT_VERSION = 1;
static const uint32_t MAX_FORMAT_VERSION = 2;
const uint8_t FixedFileTrailer::TRAILER_BLOCK_TYPE[] = { 'T', 'R', 'A', 'B', 'L', 'K', '"', '$' };
const uint8_t FixedFileTrailer::DATA_BLOCK_TYPE[] = {'D','A','T','A','B','L','K','*'};
const uint8_t FixedFileTrailer::ENCODED_DATA_BLOCK_TYPE[] = {'D','A','T','A','B','L','K','E'};
//the first element is a placeholder.
const int FixedFileTrailer::TRAILER_SIZE[]= {0,60,212};

int FixedFileTrailer::GetTrailerSize(int version)
{
    return TRAILER_SIZE[version];
}

Status FixedFileTrailer::DeserializeFromBuffer(uint8_t* buffer, int len,
        FixedFileTrailer& trailer)
{
    uint32_t* ver = reinterpret_cast<uint32_t*>(buffer + len - sizeof(uint32_t));
    uint32_t major_version = (*ver) & 0x00ffffff;
    uint32_t minor_version = (*ver) & 0xff000000;

    if (major_version < MIN_FORMAT_VERSION
            || major_version > MAX_FORMAT_VERSION)
    {
        stringstream ss;
        ss << "Invalid HFile version " + major_version
           << " (expected to be between " << MIN_FORMAT_VERSION << " and "
           << MAX_FORMAT_VERSION << ")";
        return Status(ss.str());
    }

    trailer.minor_version_ = minor_version;
    trailer.major_version_ = major_version;

    uint32_t trailer_size = GetTrailerSize(major_version);

    uint8_t* trailer_ptr = buffer + len - trailer_size;

    uint8_t* block_type_ptr = trailer_ptr;

    if (memcmp(block_type_ptr, TRAILER_BLOCK_TYPE, sizeof(TRAILER_BLOCK_TYPE)))
    {
        stringstream ss;
        ss << "Invalid magic : expected " + "TRABLK\"$"
           << ",got " + string(block_type_ptr, 8);
        return Status(ss.str());
    }

    trailer_ptr += sizeof(TRAILER_BLOCK_TYPE);

    trailer.file_info_offset_ = ReadWriteUtil::GetLongInt(trailer_ptr);
    trailer_ptr += sizeof(uint64_t);
    trailer.load_on_open_data_offset_ = ReadWriteUtil::GetLongInt(trailer_ptr);
    trailer_ptr += sizeof(uint64_t);
    trailer.data_index_count_ = ReadWriteUtil::GetInt(trailer_ptr);
    trailer_ptr += sizeof(uint32_t);

    if (trailer.major_version_ == 1)
    {
        trailer_ptr +=  sizeof(uint64_t);
    }
    else
    {
        trailer.uncompressed_data_index_size = ReadWriteUtil::GetLongInt(
                trailer_ptr);
        trailer_ptr +=  sizeof(uint64_t);
    }

    trailer.meta_index_count_ = ReadWriteUtil::GetInt(trailer_ptr);
    trailer_ptr += sizeof(uint32_t);

    trailer.total_uncompressed_bytes_ = ReadWriteUtil::GetLongInt(trailer_ptr);
    trailer_ptr += sizeof(uint64_t);

    if (trailer.major_version_ == 1)
    {
        trailer.entry_count_ = ReadWriteUtil::GetInt(trailer_ptr);
        trailer_ptr +=  sizeof(uint32_t);
    }
    else
    {
        trailer.entry_count_ = ReadWriteUtil::GetLongInt(trailer_ptr);
        trailer_ptr += sizeof(uint64_t);
    }

    trailer.compression_codec_ = ReadWriteUtil::GetInt(trailer_ptr);
    trailer_ptr +=  sizeof(uint32_t);

    if (trailer.major_version_ > 1)
    {
        trailer.num_data_index_levels_ = ReadWriteUtil::GetInt(trailer_ptr);
        trailer_ptr +=  sizeof(uint32_t);

        trailer.first_data_block_offset_ = ReadWriteUtil::GetLongInt(
                                               trailer_ptr);
        trailer_ptr += sizeof(uint64_t);

        trailer.last_data_block_offset_ = ReadWriteUtil::GetLongInt(
                                              trailer_ptr);
        trailer_ptr += sizeof(uint64_t);

        uint8_t* end_comparator_ptr = trailer_ptr + MAX_COMPARATOR_NAME_LENGTH;

        //TODO FIXME recheck this
        while (end_comparator_ptr > trailer_ptr)
        {
            if (*end_comparator_ptr != 0)
                break;
            --end_comparator_ptr;
        }
        //move end pointer one step further?
        trailer.comparator_class_name_ = std::string(trailer_ptr,end_comparator_ptr-trailer_ptr+1);

        trailer_ptr += MAX_COMPARATOR_NAME_LENGTH;
    }


    if(trailer.minor_version_ < MINOR_VERSION_WITH_CHECKSUM)
    {
        trailer.header_size_ = HEADER_SIZE_NO_CHECKSUM;
    }
    else
    {
        trailer.header_size_ = HEADER_SIZE_WITH_CHECKSUMS;
    }

    int version = ReadWriteUtil::GetInt(trailer_ptr);
    if (trailer.major_version_ != (version & 0x00ffffff))
    {
        stringstream ss;
        ss << "Invalid HFile major version " << (version & 0x00ffffff)
           << "(expected " << major_version_ << ")";
        return Status(ss.str());
    }
    if (trailer.minor_version_ != (version & 0xff000000))
    {
        stringstream ss;
        ss << "Invalid HFile minor version " << (version & 0xff000000)
           << "(expected " << minor_version << ")";
        return Status(ss.str());
    }
    return Status::OK;
}

}

