/*
 * hdfs-hfile-scanner.cc
 *
 *  Created on: 2013-5-9
 *      Author: Rao
 */

#include "exec/hdfs-hfile-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/read-write-util.h"
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/jni-util.h"
#include "exec/hfile-types.h"
#include "algorithm"
#include "exec/hdfs-scan-node.h"

#include <boost/scoped_ptr.hpp>

using namespace std;
using namespace impala;
using namespace hfile;


class KeyValue
{

public:
    KeyValue():key_start_ptr_(NULL),key_len_(-1),value_start_ptr_(NULL),value_len_(-1)
    {
    }
    void Set_Key_State(std::vector<PrimitiveType>& types,std::vector<SlotDescriptor*> & slot_desc)
    {
        key_deserializer_.Set_State(types,slot_desc);
    }
    void Set_Value_State(std::vector<PrimitiveType>& types,std::vector<SlotDescriptor*>& slot_desc)
    {
        value_deserializer_.Set_State(types,slot_desc);
    }
    bool Write_Tuple(MemPool* pool,Tuple* tuple,uint8_t** byte_buffer_ptr);
    int Get_Key_Col_Num(uint8_t* data,int len,PrimitiveType* types)
    {
        Parse_Key_Value(&data);
        return key_deserializer_.Get_Key_Col_Num(data,len,types);
    }

private:
    inline	void Parse_Key_Value(uint8_t** byte_buffer_ptr);

    BinarySortableDeserializer key_deserializer_;
    LazyBinaryDeserializer value_deserializer_;
    uint8_t* key_start_ptr_;
    int key_len_;
    uint8_t* value_start_ptr_;
    int value_len_;
}

void KeyValue::Parse_Key_Value(uint8_t** byte_buffer_ptr)
{
    key_len_ = ReadWriteUtil::GetInt(*byte_buffer_ptr);
    *byte_buffer_ptr+=4;
    value_len_ = ReadWriteUtil::GetInt(*byte_buffer_ptr);
    *byte_buffer_ptr+=4;
    key_start_ptr_= *byte_buffer_ptr;
    *byte_buffer_ptr+=key_len_;
    value_start_ptr_ = *byte_buffer_ptr;
    *byte_buffer_ptr += value_len_;
    //skip memstore timestamp
    int8_t vlong_len = *static_cast<int8_t*>(*byte_buffer_ptr);
    *byte_buffer_ptr+=ReadWriteUtil::DecodeVIntSize(vlong_len);
    //adjust key_start_ptr_ to point to row key start position.
    key_len_ =   ReadWriteUtil::GetSmallInt(key_start_ptr_);
    key_start_ptr_+=2;
}

bool KeyValue:: Write_Tuple(MemPool* pool,Tuple* tuple,uint8_t** byte_buffer_ptr)
{
    Parse_Key_Value(byte_buffer_ptr);
    bool result = true;
    result &=key_deserializer_.Write_Tuple(pool, tuple, key_start_ptr_,key_len_);
    result &= value_deserializer_.Write_Tuple(pool,tuple,value_start_ptr_,value_len_);
    return result;
}



class Deserializer
{
public:
    virtual ~Deserializer() {}

    //in most cases, this should be call once.
    void Set_State(std::vector<PrimitiveType>& types, std::vector<SlotDescriptor*>& slot_desc)
    {
        types_ = types;
        slot_desc_ = slot_desc;
    }
    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len) = 0;

protected:
    std::vector<PrimitiveType> types_;
    std::vector<SlotDescriptor*> slot_desc_;
private:
    Deserializer(const Deserializer&);
    Deserializer& operator=(const Deserializer&);
}


class LazyBinaryDeserializer:public Deserializer
{
public:
    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len);
private:
    bool Write_Field(MemPool* pool,Tuple*tuple,uint8_t** data,PrimitiveType type,SlotDescriptor* slot_desc);
}

//row key part of KeyValue use BinarySortableSerDe to encode it's data.
class BinarySortableDeserializer:public Deserializer
{
public:

    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len);
private:
    friend class KeyValue;

    bool Write_Field(MemPool* pool,Tuple*tuple,uint8_t** data,PrimitiveType type,SlotDescriptor* slot_desc);
    int Get_Key_Col_Num(uint8_t* data, int len, PrimitiveType* types);
}


bool LazyBinaryDeserializer::Write_Field(MemPool* pool,Tuple*tuple,uint8_t** data,PrimitiveType type,SlotDescriptor* slot_desc)
{

    void*slot = NULL;
    if(slot_desc)
    {
        slot = tuple->GetSlot(slot_desc->tuple_offset());
    }

    switch(type)
    {
    case TYPE_BOOLEAN:

        if(slot)
        {
            *reinterpret_cast<bool*>( slot) = (**data == 0?false:true);
        }
        ++*data;
        break;

    case TYPE_TINYINT:

        if(slot)
        {
            *reinterpret_cast<int8_t*> (slot) = **data;
        }
        ++*data;

        break;

    case TYPE_SMALLINT:

        if(slot)
        {
            *reinterpret_cast<int8_t*>(slot) =ReadWriteUtil::GetSmallInt(*data);
        }

        *data+=2;
        break;

    case TYPE_INT:

        if(slot)

        {
            int len = ReadWriteUtil::GetVInt(uint8_t * data, reinterpret_cast<int32_t*>(slot));
            *data+=len;
        }
        else
        {
            int len = ReadWriteUtil::DecodeVIntSize(**data);
            *data+=len;
        }

        break;


    case TYPE_FLOAT:


        if(slot)
        {
            int32_t val = ReadWriteUtil::GetInt(*data);

            *reinterpret_cast<float*>(slot) =*(reinterpret_cast<float*>(&val));
        }

        *data+=4;
        break;

    case TYPE_BIGINT:

        if(slot)
        {
            int len = ReadWriteUtil::GetVLong(*data, reinterpret_cast<int64_t*>( slot))
                      *data+=len;

        }
        else
        {
            int len = ReadWriteUtil::DecodeVIntSize(**data);
            *data+=len;
        }

        break;

    case TYPE_DOUBLE:

        int64_t val = ReadWriteUtil::GetLongInt(*data);

        *reinterpret_cast<double*>( slot) = *(reinterpret_cast<double*>(&val));

        *data+=8;
        break;

    case TYPE_STRING:
    {

        StringValue* sv = reinterpret_cast<StringValue*>(slot);
        *data+=ReadWriteUtil::GetVInt(*data, reinterpret_cast<int32_t*>(&sv->len))


               if (stream_->compact_data() && sv->len > 0)
        {
            sv->ptr = reinterpret_cast<char*>(pool->Allocate(sv->len));
            memcpy(sv->ptr, *data, sv->len);
        }
        else
        {
            sv->ptr = reinterpret_cast<char*>(*data);
        }
        break;
    }

    default:
        DCHECK(false);
    }

}

bool LazyBinaryDeserializer::Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len)
{

    uint8_t* const data_end_ptr = data +len;
    uint8_t null_byte = *data++;
    int fields = 0;
    for(int i =0; i < types_.size(); i ++)
    {
        //this field is null
        if(null_byte&(1<<(i % 8)) == 0)
        {
            //this field need materialized
            if(slot_desc_[i])
            {
                tuple->SetNull((slot_desc_[i]->null_indicator_offset());
            }
        }
        else//there is data exist for this field, materialized it or skip it based on whether slot_desc == NULL or not.
        {
            Write_Field(pool,tuple,&data,types_[i],slot_desc_[i]);
        }

        if(data <= data_end_ptr)
        {
            fields++;
        }

        if(7 == (i%8))
        {
            if(data < data_end_ptr)
            {
                null_byte = *data++;
            }
            else
            {
                null_byte = 0;
            }
        }

    }
    DCHECK(data == data_end_ptr);
}

bool BinarySortableDeserializer::Write_Field(MemPool * pool, Tuple * tuple, uint8_t ** data, PrimitiveType type, SlotDescriptor * slot_desc)
{

    //first check null indicator.
    uint8_t is_null = **data;
    ++*data;
    if(is_null == 0)
    {
        if(slot_desc)
        {
            tuple->SetNull(slot_desc->null_indicator_offset());
        }
        return true;
    }

    DCHECK( is_null == 1);
    void* slot = NULL;
    if(slot_desc)
    {
        slot = tuple->GetSlot(slot_desc->tuple_offset());
    }
    switch(type)
    {

    case TYPE_BOOLEAN:
    {

        if(slot)
        {
            uint8_t* b = *data;
            DCHECK(b==1 || b ==2);
            if(b == 1)
            {
                *reinterpret_cast<bool*>slot = 0;
            }
            else
            {
                *reinterpret_cast<bool*>slot = 1;
            }

        }
        ++*data;

        break;
    }
    case TYPE_TINYINT:
        if(slot)
        {
            *reinterpret_cast<int8_t*>(slot) = *data ^ 0x80;

        }

        ++*data;
        break;

    case TYPE_SMALLINT:
        if(slot)
            *reinterpret_cast<int16_t*>(slot) = ((*data)[0] ^0x80)<<8 | (*data)[1];

        *data +=2;
        break;

    case TYPE_INT:
        if(slot)
            *reinterpret_cast<int32_t*>(slot) =  ((*data)[0] ^ 0x80)<<24|(*data)[1]<<16|(*data)[2]<<8|(*data)[3];
        *data+=4;
        break;

    case TYPE_BIGINT:
        if(slot)
            *reinterpret_cast<int64_t*>(slot) =(static_cast<int64_t>((*data)[0] ^ 0x80) << 56) |
                                               (static_cast<int64_t>((*data)[1]) << 48) |
                                               (static_cast<int64_t>((*data)[2]) << 40) |
                                               (static_cast<int64_t>((*data)[3]) << 32) |
                                               ((*data)[4] << 24) | ((*data)[5] << 16) | ((*data)[6] << 8) | (*data)[7];
        *data += 8;
        break;

    case TYPE_FLOAT:

        if(slot)
        {
            int32_t value = ReadWriteUtil::GetInt(*data);
            if(value & (1<<31) == 0)
            {
                value = ~value;
            }
            else
            {
                value^=(1<<31);
            }
            *reinterpret_cast<float*>(slot) = *reinterpret_cast<float*>(&value);
        }
        *data += 4;
        break;

    case TYPE_DOUBLE:
        if(slot)
        {

            int64_t value = ReadWriteUtil::GetLongInt(*data);
            if(value &(static_cast<int64_t>(1)<<63) ==0 )
            {
                value = ~value;
            }
            else
            {
                value = value ^(static_cast<int64_t>(1)<<63);
            }
            *reinterpret_cast<double*>(slot) = *reinterpret_cast<double*>(&value);
        }
        *data += 8;
        break;

    case TYPE_STRING:
    {
        int len_str = 0;
        uint8_t*const str_start_ptr = *data;
        uint8_t b;
        do
        {
            b = *(*data)++;
            if(b == 0)
                break;
            if(b ==1)
                ++*data;
            len_str++;
        }
        while(true);

        if(slot)
        {

            StringValue* sv = reinterpret_cast<StringValue*>(slot);
            sv->len = len_str;
            if(len_str == ((*data) - str_start_ptr))
            {
                if (stream_->compact_data() && sv->len > 0)
                {
                    sv->ptr = reinterpret_cast<char*>(pool->Allocate(len_str));
                    memcpy(sv->ptr, str_start_ptr, sv->len);
                }
                else
                {
                    sv->ptr = reinterpret_cast<char*>(str_start_ptr);
                }
            }
            else
            {
                //escaping happened,
                uint8_t* const str_real = pool->Allocate(len_str);
                uint8_t* start_ptr=const_cast<uint8_t*> str_start_ptr;
                uint8_t b;
                for(int i = 0 ; i < len_str; i++)
                {
                    b = *start_ptr++;
                    if(b  == 1)
                    {
                        b = *start_ptr++ -1;
                    }
                    str_real[i] = b;
                }
                sv->ptr = str_real;
            }
        }
        break;

    }
    default:
        DCHECK(false);

    }
    return true;

}

int BinarySortableDeserializer::Get_Key_Col_Num(uint8_t* data,int len,PrimitiveType* types)
{
    uint8_t* data_cur_ptr = data;
    uint8_t* const data_end_ptr = data + len;
    int col_count = 0;
    while(data_cur_ptr != data_end_ptr)
    {
        Write_Field(NULL,NULL, &data_cur_ptr, *types, NULL);
        ++types;
        ++col_count;
    }
    DCHECK(data_cur_ptr == data_end_ptr);

    return col_count;
}

bool BinarySortableDeserializer:: Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len)
{
    uint8_t* data_cur_ptr = data;
    uint8_t* data_end_ptr = data + len;

    for(int i = 0 ; i < types_.size(); i ++)
    {
        Write_Field(pool, tuple, &data_cur_ptr, types_[i], slot_desc_[i]);
    }
    DCHECK(data_cur_ptr == data_end_ptr);
    return true;
}



impala::HdfsHFileScanner::HdfsHFileScanner(HdfsScanNode* scan_node, RuntimeState* state) :
    HdfsScanner(scan_node, state),byte_buffer_ptr_(NULL),byte_buffer_end_(NULL),num_checksum_bytes_(0),num_key_cols_(-1),only_parsing_trailer_(false)
{
}

impala::HdfsHFileScanner::~HdfsHFileScanner()
{
}


Status HdfsHFileScanner::ProcessSplit(ScannerContext* context)
{

    SetContext(context);

    HdfsFileDesc* file_desc = scan_node_->GetFileDesc(stream_->filename());
    DCHECK(file_desc != NULL);

    //each scanner object associated with a scanner thread in current context.
    //
    //keep trailer_ in per file meta data in order to let another scan range get access to it.
    trailer_ = reinterpret_cast<hfile::FixedFileTrailer*>(scan_node_->GetFileMetadata(
                   stream_->filename()));

    if (trailer_ == NULL)
    {
        //this is the initial scan range just to parse the trailer
        only_parsing_trailer_ = true;

        trailer_ = state_->obj_pool()->Add(new hfile::FixedFileTrailer());

        RETURN_IF_ERROR(ProcessTrailer());

        scan_node_->SetFileMetadata(stream_->filename(), trailer_);

        //release scanner thread after done its work.
        scan_node_->runtime_state()->resource_pool()->ReleaseThreadToken(false);

        //before this scan thread die, it will pass queued scan ranges to disk io manager.
        return IssueFileRanges(stream_->filename());
    }

    only_parsing_trailer_ = false;

    kv_parser.reset(new KeyValue());

    RETURN_IF_ERROR(ProcessSplitInternal());

    return Status::OK;
}


Status HdfsHFileScanner::ProcessTrailer()
{

    uint8_t* buffer;
    int len;
    bool eos;

    if (!stream_->GetBytes(0, &buffer, &len, &eos, &parse_status_))
    {
        return parse_status_;
    }
    //sure to end of file.
    DCHECK(eos);

    DCHECK_GE(len, FixedFileTrailer::MAX_TRAILER_SIZE);

    RETURN_IF_ERROR(hfile::FixedFileTrailer::DeserializeFromBuffer(buffer, len, *trailer_));

    return Status::OK;

}



Status HdfsHFileScanner::ProcessSplitInternal()
{
    while(!scan_node_->ReachedLimit() && !context_->cancelled())
    {

        MemPool* pool;
        Tuple* tuple;
        TupleRow* row;
        int num_rows = context_->GetMemory(&pool, &tuple, &row);
        int num_to_commit = 0;

        for(int i = 0; i < num_rows; i++)
        {
            InitTuple(context_->template_tuple(), tuple);
            if(!WriteTuple(pool, tuple))
            {
                context_->CommitRows(num_to_commit);

                return parse_status_;
            }
            row->SetTuple(scan_node_->tuple_idx(), tuple);
            if(ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, row))
            {
                row = context_->next_row(row);
                tuple = context_->next_tuple(tuple);
                ++num_to_commit;
            }
        }
        context_->CommitRows(num_to_commit);
    }

    if(context_->cancelled())
        return Status::CANCELLED;
    return parse_status_;

}



Status HdfsHFileScanner::Prepare()
{
    const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();
//    const std::vector<SlotDescriptor*>& slots = tuple_desc->slots();
    const HdfsTableDescriptor* hdfs_table = static_cast<const HdfsTableDescriptor*>(tuple_desc->table_desc());
    col_types_ = hdfs_table->col_types();
    num_clustering_cols_ = hdfs_table->num_clustering_cols();
    return Status::0K;
}

bool HdfsHFileScanner::WriteTuple(MemPool * pool, Tuple * tuple)
{

    if(byte_buffer_ptr_ == byte_buffer_end_)
    {
        if(num_checksum_bytes_ > 0)
        {
            Status dumy_status;
            if(!stream_->SkipBytes(num_checksum_bytes_, &dumy_status))
            {
                parse_status_ = dumy_status;
                return false;
            }
        }
        Status s = ReadDataBlock();
        if(!s.ok())
            return false;
        if(byte_buffer_ptr_== byte_buffer_end_)
            return false;
    }

    //de-serialize a record beforehand to get number of columns constituting key and value ,respectively.
    if(UNLIKELY(num_key_cols_ == -1))
    {
 	const  std::vector<SlotDescriptor*>& materailized_slots = scan_node_->materialized_slots();
        std::vector<PrimitiveType> key_types;
        std::vector<SlotDescriptor*> key_slot_desc;
        std::vector<PrimitiveType> value_types;
        std::vector<SlotDescriptor*> value_slot_desc;

        num_key_cols_ = kv_parser->Get_Key_Col_Num(byte_buffer_ptr_,&col_types_[num_clustering_cols_]);
        for(int i = num_clustering_cols_; i < (num_clustering_cols_+num_key_cols_); i++)
        {
            key_types.push_back((col_types_)[i]);
            int index_slot = scan_node_->GetMaterializedSlotIdx(i);
            if(index_slot != HdfsScanNode::SKIP_COLUMN)
            {
                key_slot_desc.push_back(materailized_slots[index_slot]);
            }
            else
            {
                key_slot_desc.push_back(NULL);
            }
        }

        for(int i = num_clustering_cols_+num_key_cols_; i< (col_types_).size(); i++)
        {
            value_types.push_back((col_types_)[i]);
            int index_slot = scan_node_->GetMaterializedSlotIdx(i);
            if(index_slot != HdfsScanNode::SKIP_COLUMN)
            {
                value_slot_desc.push_back(materailized_slots[index_slot]);
            }
            else
            {
                value_slot_desc.push_back(NULL);
            }
        }
        kv_parser->Set_Key_State(key_types,key_slot_desc);
        kv_parser->Set_Value_State(value_types,value_slot_desc);
    }


    return kv_parser->Write_Tuple(pool,Tuple,&byte_buffer_ptr_);

}


Status HdfsHFileScanner::ReadDataBlock()
{
    Status status;
    uint8_t* buffer;
    int num_bytes;
    bool eos;

    while(true)
    {

        if(!stream_->GetBytes(trailer_->header_size_, &buffer, &num_bytes, &eos, &status))
            return status;

        DCHECK_EQ(trailer_.header_size_, num_bytes);

        if(stream_->file_offset() - num_bytes >  trailer_.last_data_block_offset_)
        {
            //has already read all data blocks
            break;
        }


        uint8_t* block_type = buffer;
        buffer += 8;
        uint32_t on_disk_size = ReadWriteUtil::GetInt(buffer);
        buffer += 4;
        uint32_t uncompressed_size = ReadWriteUtil::GetInt(buffer);
        buffer += 4;
        //skip previous block offset field
        buffer += 8;

        uint8_t checksum_type;
        uint32_t bytes_per_checksum;
        uint32_t on_disk_size_with_header;

        if(trailer_.minor_version_  >=  FixedFileTrailer::MINOR_VERSION_WITH_CHECKSUM)

        {
            checksum_type = *buffer;
            buffer++;
            bytes_per_checksum=ReadWriteUtil::GetInt(buffer);
            buffer+=4;
            on_disk_size_with_header=ReadWriteUtil::GetInt(buffer);
            buffer+=4;
        }
        else
        {
            checksum_type=0;
            bytes_per_checksum = 0;
            on_disk_size_with_header = on_disk_size + FixedFileTrailer::HEADER_SIZE_NO_CHECKSUM;
        }

        //it is suffice to  compare prefix to determine whether this block is a data block
        if(memcmp(block_type,FixedFileTrailer::DATA_BLOCK_TYPE,7))
        {
            //this block is not a data block, skip this block and continue;
            if(!stream_->SkipBytes(on_disk_size, &status))
                return status;
            continue;
        }

        //it's really a data block.
        if(!stream_->ReadBytes(uncompressed_size, &buffer, &status))
        {
            return status;
        }

        byte_buffer_ptr_ = buffer;
        byte_buffer_end_ = buffer + uncompressed_size;


        num_checksum_bytes_ = 0;
        if(checksum_type)
        {
            num_checksum_bytes_  = on_disk_size-uncompressed_size;
        }

    }

    return Status::OK;
}


//put scan range into queue, which will be issued to io manager when scanner thread terminate.
Status HdfsHFileScanner::IssueFileRanges(const char* filename)
{

    HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename);

    ScanRangeMetadata* metadata =
        reinterpret_cast<ScanRangeMetadata*>(file_desc->splits[0]->meta_data());

    DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(filename, file_desc->file_length,
                                  trailer_->first_data_block_offset_, metadata->partition_id, -1);
    scan_node_->AddDiskIoRange(range);

    return Status::OK;

}

void HdfsHFileScanner::IssueInitialRanges(HdfsScanNode* scan_node,
        const std::vector<HdfsFileDesc*>& files)
{
    for (int i = 0; i < files.size(); ++i)
    {
        for (int j = 0; j < files[i]->splits.size(); ++j)
        {
            DiskIoMgr::ScanRange* split = files[i]->splits[j];

            // Since hfile scanners always read entire files, only read a file if we're
            // assigned the first split
            //to avoid duplicate reading file many times.
            if (split->offset() != 0)
            {
                // We assign the entire file to one scan range, so mark all but one split
                // (i.e. the first split) as complete
                //FIXME
                //add a new enum value for our hfile.
                scan_node->RangeComplete(THdfsFileFormat::HFILE, THdfsCompression::NONE);
                continue;
            }

            // Compute the offset of the file footer
            DCHECK_GT(files[i]->file_length, 0);
            int64_t trailer_start = max(0L,files[i]->file_length - FixedFileTrailer::MAX_TRAILER_SIZE);

            ScanRangeMetadata* metadata =
                reinterpret_cast<ScanRangeMetadata*>(files[i]->splits[0]->meta_data());
            DiskIoMgr::ScanRange* footer_range = scan_node->AllocateScanRange(
                    files[i]->filename.c_str(), FixedFileTrailer::MAX_TRAILER_SIZE, trailer_start,
                    metadata->partition_id, files[i]->splits[0]->disk_id());
            scan_node->AddDiskIoRange(footer_range);
        }
    }
}


