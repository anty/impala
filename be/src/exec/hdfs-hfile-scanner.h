/*
 * hdfs-hfile-scanner.h
 *
 *  Created on: 2013-5-9
 *      Author: Rao
 */

#ifndef HDFS_HFILE_SCANNER_H_
#define HDFS_HFILE_SCANNER_H_

#include "exec/hdfs-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hfile-types.h"
#include "util/codec.h"
#include "exec/read-write-util.h"

#include "exec/hfile-types.h"
#include <boost/scoped_ptr.hpp>

namespace impala
{

class RuntimeState;
class MemPool;
class Status;
using namespace hfile;
//Scanner used to parse HFile file format.

class HdfsHFileScanner: public HdfsScanner
{
public:
    HdfsHFileScanner(HdfsScanNode* scan_node, RuntimeState* state);
    virtual ~HdfsHFileScanner();

    // Issue io manager byte ranges for 'files'
    static void IssueInitialRanges(HdfsScanNode*,const std::vector<HdfsFileDesc*>& files);

    // Implementation of HdfsScanner interface.
    virtual Status Prepare();
    virtual Status ProcessSplit(ScannerContext* context);
    virtual Status Close();

//	static Status Init();

private:

    struct BlockHeader
    {
        uint8_t* block_type_;
        uint32_t on_disk_size_without_header_;
        uint32_t uncompressed_size_without_header_;
        uint8_t checksum_type_;
        uint32_t bytes_per_checksum_;
        uint32_t on_disk_data_size_with_header_;
        uint32_t on_disk_data_size_without_header_;
        uint32_t checksum_size_;
    };
    BlockHeader block_header_;
    bool read_block_header_;

    void Read_Header(uint8_t*&buffer, int minor_version)
    {
        block_header_.block_type_= buffer;
        buffer += 8;
        block_header_.on_disk_size_without_header_= ReadWriteUtil::GetInt(buffer);
        buffer += 4;
        block_header_.uncompressed_size_without_header_= ReadWriteUtil::GetInt(buffer);
        buffer += 4;
        //skip previous block offset field
        buffer += 8;
        if(trailer_->minor_version_  >=  hfile::FixedFileTrailer::MINOR_VERSION_WITH_CHECKSUM)
        {
            block_header_.checksum_type_= *buffer;
            buffer++;
            block_header_.bytes_per_checksum_=ReadWriteUtil::GetInt(buffer);
            buffer+=4;
            block_header_.on_disk_data_size_with_header_=ReadWriteUtil::GetInt(buffer);
            buffer+=4;
        }
        else
        {
            block_header_.checksum_type_=0;
            block_header_.bytes_per_checksum_= 0;
            block_header_.on_disk_data_size_with_header_= block_header_.on_disk_size_without_header_+ hfile::FixedFileTrailer::HEADER_SIZE_NO_CHECKSUM;
        }
        block_header_.on_disk_data_size_without_header_= block_header_.on_disk_data_size_with_header_-trailer_->header_size_;
        block_header_.checksum_size_= block_header_.on_disk_size_without_header_- block_header_.on_disk_data_size_without_header_;
    }


    Status ProcessTrailer();
    Status ReadDataBlock();
    Status IssueFileRanges(const char* filename);
    bool WriteTuple(MemPool* pool, Tuple* tuple,bool skip);
    Status ProcessSplitInternal();


    boost::scoped_ptr<MemPool> decompressed_data_pool_;
    boost::scoped_ptr<Codec> decompressor_;

    uint8_t* block_buffer_;
    int32_t block_buffer_len_;

    uint8_t* byte_buffer_ptr_;
    uint8_t* byte_buffer_end_;

    std::vector<PrimitiveType> col_types_;
    std::vector<PrimitiveType>  key_col_types_;
    std::vector<PrimitiveType>  value_col_types_;
    //in current implementation, there is no easy to figure out the number of columns in key part of a KeyValue object.
    //So we de-serialize a record to get this number.
    int num_key_cols_;
    int num_clustering_cols_;
    class KeyValue;
    boost::scoped_ptr<KeyValue> kv_parser_;
    hfile::FixedFileTrailer* trailer_;
    bool only_parsing_trailer_;

};

} //namespace impala

#endif /* HDFS_HFILE_SCANNER_H_ */
