/*
 * hdfs-hfile-scanner.cc
 *
 *  Created on: 2013-5-9
 *      Author: Rao
 */
#include "exec/scanner-context.inline.h"
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
#include "exec/hdfs-scanner.h"
#include <snappy.h>
#include "util/codec.h"
#include <boost/scoped_ptr.hpp>
#include <math.h>

using namespace std;
using namespace impala;
using namespace hfile;


namespace
{

class Deserializer
{
public:
    virtual ~Deserializer() {}

    //in most cases, this should be call once.
    void Set_State(std::vector<PrimitiveType>& types, std::vector<SlotDescriptor*>& slot_desc,bool compact_data)
    {
        types_ = types;
        slot_desc_ = slot_desc;
        compact_data_ = compact_data;
    }
    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len) = 0;
protected:
    std::vector<PrimitiveType> types_;
    std::vector<SlotDescriptor*> slot_desc_;
    bool compact_data_;
};


class LazyBinaryDeserializer:public Deserializer
{
public:
    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len);
private:
    bool Write_Field(MemPool* pool,Tuple*tuple,uint8_t** data,PrimitiveType type,SlotDescriptor* slot_desc);
};


bool LazyBinaryDeserializer::Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len)
{

    uint8_t* const data_end_ptr = data +len;
    uint8_t null_byte = *data++;
    int fields = 0;
    for(int i =0; i < types_.size(); i ++)
    {
        //this field is null
        if((null_byte&(1<<(i % 8))) == 0)
        {
            //this field need materialized
            if(slot_desc_[i])
            {
                tuple->SetNull(slot_desc_[i]->null_indicator_offset());
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
    return true;
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
            *reinterpret_cast<int8_t*>(slot) =ReadWriteUtil::GetInt<uint16_t>(*data);
        }

        *data+=2;
        break;

    case TYPE_INT:

        if(slot)

        {
            int len = ReadWriteUtil::GetVInt(* data, reinterpret_cast<int32_t*>(slot));
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
            uint32_t val = ReadWriteUtil::GetInt<uint32_t>(*data);

            *reinterpret_cast<float*>(slot) =*(reinterpret_cast<float*>(&val));
        }

        *data+=4;
        break;

    case TYPE_BIGINT:

        if(slot)
        {
            int len = ReadWriteUtil::GetVLong(*data, reinterpret_cast<int64_t*>( slot));
            *data+=len;

        }
        else
        {
            int len = ReadWriteUtil::DecodeVIntSize(**data);
            *data+=len;
        }
        break;

    case TYPE_DOUBLE:
    {
        if(slot)
        {
            int64_t val = ReadWriteUtil::GetInt<int64_t>(*data);
            *reinterpret_cast<double*>( slot) = *(reinterpret_cast<double*>(&val));
        }
        *data+=8;
        break;
    }
    case TYPE_STRING:
    {
        int32_t len;
        *data+= ReadWriteUtil::GetVInt(*data, &len);
        if(slot)
        {
            StringValue* sv = reinterpret_cast<StringValue*>(slot);
            sv->len = len;
            if (compact_data_&& sv->len > 0)
            {
                sv->ptr = reinterpret_cast<char*>(pool->Allocate(sv->len));
                memcpy(sv->ptr, *data, sv->len);
            }
            else
            {
                sv->ptr = reinterpret_cast<char*>(*data);
            }
        }
        *data+=len;
        break;
    }
    case TYPE_TIMESTAMP:
    {
        uint32_t tmp =  ReadWriteUtil::GetInt<uint32_t>(*data);
        *data+=4;
        bool has_decimal = tmp & 0x80000000;
        int32_t time = tmp & 0x7FFFFFFF;
        int32_t nano = 0;
        if(has_decimal)
        {
            *data += ReadWriteUtil::GetVInt(*data,&nano);
        }
        if(slot)
        {
            if(nano != 0)
            {
                int len = static_cast<int32_t>(floor(log10(nano))) + 1;
                int tmp = 0;
                while(nano != 0)
                {
                    tmp *= 10;
                    tmp+=nano %10;
                    nano /=10;
                }
                nano = tmp;
                if(len < 9)
                {
                    nano *= pow(10,9-len);
                }
            }
            *reinterpret_cast<TimestampValue*>(slot) = TimestampValue(static_cast<int64_t>(time)
                    ,static_cast<int64_t>(nano));
        }
        break;
    }
    default:
        DCHECK(false);
    }
    return true;
}


//row key part of KeyValue use BinarySortableSerDe to encode it's data.
class BinarySortableDeserializer:public Deserializer
{
public:
    virtual bool Write_Tuple(MemPool* pool,Tuple*tuple,uint8_t* data ,int len);
    int Get_Key_Col_Num(uint8_t* data, int len, PrimitiveType* types);
private:
    bool Write_Field(MemPool* pool,Tuple*tuple,uint8_t** data,PrimitiveType type,SlotDescriptor* slot_desc);

};

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
            DCHECK(*b==1 || *b ==2);
            if(*b == 1)
            {
                *reinterpret_cast<bool*>(slot) = false;
            }
            else
            {
                *reinterpret_cast<bool*>(slot) = true;
            }

        }
        ++*data;

        break;
    }
    case TYPE_TINYINT:
        if(slot)
        {
            *reinterpret_cast<int8_t*>(slot) = (**data) ^ 0x80;
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
                                               (static_cast<int64_t>((*data)[4]) << 24) |
                                               (static_cast<int64_t>((*data)[5]) << 16) |
                                               (static_cast<int64_t>((*data)[6]) << 8) |
                                               (*data)[7];
        *data += 8;
        break;

    case TYPE_FLOAT:

        if(slot)
        {
            int32_t value = ReadWriteUtil::GetInt<uint32_t>(*data);
            if((value & (1<<31)) == 0)
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
            int64_t value = ReadWriteUtil::GetInt<int64_t>(*data);
            if((value &(static_cast<int64_t>(1)<<63)) ==0 )
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
        uint8_t* str_start_ptr = *data;
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
                if (compact_data_&& sv->len > 0)
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
                uint8_t* str_real = pool->Allocate(len_str);
                uint8_t* start_ptr=  str_start_ptr;
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
                sv->ptr = reinterpret_cast<char*>(str_real);
            }
        }
        break;

    }
    case TYPE_TIMESTAMP:
    {
        //serialized sortable timestamp always contains both time and fractional part.
        int32_t time = ReadWriteUtil::GetInt<uint32_t>(*data) & 0x7FFFFFFF;
        *data+=4;
        int32_t nano = ReadWriteUtil::GetInt<uint32_t>(*data);
        *data+=4;
        *reinterpret_cast<TimestampValue*>(slot) = TimestampValue(static_cast<int64_t>(time)
                ,static_cast<int64_t>(nano));
        break;
    }
    default:
        DCHECK(false);

    }
    return true;

}

}


class HdfsHFileScanner:: KeyValue
{

public:
    KeyValue():key_deserializer_(),value_deserializer_()
    {
    }
    void Set_Key_State(std::vector<PrimitiveType>& types,std::vector<SlotDescriptor*> & slot_desc,bool compact_data)
    {
        key_deserializer_.Set_State(types,slot_desc,compact_data);
    }
    void Set_Value_State(std::vector<PrimitiveType>& types,std::vector<SlotDescriptor*>& slot_desc,bool compact_data)
    {
        value_deserializer_.Set_State(types,slot_desc,compact_data);
    }
    bool Write_Tuple(MemPool* pool,Tuple* tuple,uint8_t** byte_buffer_ptr)
    {
        uint8_t* key_start_ptr;
        int key_len;
        uint8_t* value_start_ptr;
        int value_len;
        Parse_Key_Value(byte_buffer_ptr,&key_start_ptr,&key_len,&value_start_ptr,&value_len);
        bool result = true;
        result &=key_deserializer_.Write_Tuple(pool, tuple, key_start_ptr,key_len);
        result &= value_deserializer_.Write_Tuple(pool,tuple,value_start_ptr,value_len);
        return result;
    }
    int Get_Key_Col_Num(uint8_t* data,PrimitiveType* types)
    {
        uint8_t* key_start_ptr;
        int key_len;
        uint8_t* value_start_ptr;
        int value_len;
        Parse_Key_Value(&data,&key_start_ptr,&key_len,&value_start_ptr,&value_len);

        return key_deserializer_.Get_Key_Col_Num(key_start_ptr,key_len,types);
    }

private:
    void Parse_Key_Value(uint8_t** byte_buffer_ptr,uint8_t** key_start_ptr,int* key_len,uint8_t**value_start_ptr,int* value_len)
    {
        *key_len = ReadWriteUtil::GetInt<uint32_t>(*byte_buffer_ptr);
        *byte_buffer_ptr+=4;
        *value_len = ReadWriteUtil::GetInt<uint32_t>(*byte_buffer_ptr);
        *byte_buffer_ptr+=4;
        *key_start_ptr= *byte_buffer_ptr;
        *byte_buffer_ptr+=*key_len;
        *value_start_ptr = *byte_buffer_ptr;
        *byte_buffer_ptr += *value_len;
        //skip memstore timestamp
        int8_t vlong_len = **byte_buffer_ptr;
        *byte_buffer_ptr+=ReadWriteUtil::DecodeVIntSize(vlong_len);
        //adjust key_start_ptr_ to point to row key start position.
        *key_len =   ReadWriteUtil::GetInt<uint16_t>(*key_start_ptr);
        *key_start_ptr+=2;
    }
    BinarySortableDeserializer key_deserializer_;
    LazyBinaryDeserializer value_deserializer_;
};

impala::HdfsHFileScanner::HdfsHFileScanner(HdfsScanNode* scan_node, RuntimeState* state) :
    HdfsScanner(scan_node, state),byte_buffer_ptr_(NULL),byte_buffer_end_(NULL),num_key_cols_(-1),only_parsing_trailer_(false),
    decompressed_data_pool_(new MemPool(scan_node->runtime_state()->mem_limits())),read_block_header_(false),block_buffer_len_(0)
{
}

impala::HdfsHFileScanner::~HdfsHFileScanner()
{
}


Status HdfsHFileScanner::ProcessSplit()
{

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

        bool eosr = false;
        RETURN_IF_ERROR(ProcessTrailer(&eosr));

        if(eosr)
        {
            only_parsing_trailer_ = false;
            return Status::OK;
        }
        scan_node_->SetFileMetadata(stream_->filename(), trailer_);

        //release scanner thread after done its work.
        //FIXME XXX
//        scan_node_->runtime_state()->resource_pool()->ReleaseThreadToken(false);

        //before this scan thread die, it will pass queued scan ranges to disk io manager.
        return IssueFileRanges(stream_->filename());
    }

    only_parsing_trailer_ = false;

    kv_parser_.reset(new KeyValue());
    InitKeyValue();
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
                    decompressed_data_pool_.get(), stream_->compact_data(),
                    Codec::SNAPPY_COMPRESSION, &decompressor_));
    RETURN_IF_ERROR(ProcessSplitInternal());

    return Status::OK;
}

void HdfsHFileScanner ::InitKeyValue()
{
    const  std::vector<SlotDescriptor*>& materailized_slots = scan_node_->materialized_slots();

    std::vector<PrimitiveType> key_types;
    std::vector<SlotDescriptor*> key_slot_desc;
    key_types.resize(key_col_names_.size());
    key_slot_desc.resize(key_col_names_.size());

    std::vector<PrimitiveType> value_types;
    std::vector<SlotDescriptor*> value_slot_desc;

    for(int i = num_clustering_cols_; i< col_types_.size(); i++)
    {
        string col_name=col_names_[i];
        int index_slot = scan_node_->GetMaterializedSlotIdx(i);
        int index_key_col = 0;
        for(; index_key_col < key_col_names_.size(); ++index_key_col)
        {
            if(key_col_names_[index_key_col].compare(col_name) ==0)
                break;
        }
        //this is a  column comprise row key part
        if(index_key_col < key_col_names_.size())
        {
            key_types[index_key_col] = col_types_[i];
            if(index_slot != HdfsScanNode::SKIP_COLUMN)
            {
                key_slot_desc[index_key_col] = materailized_slots[index_slot];
            }
        }
        else
        {
            value_types.push_back(col_types_[i]);
            if(index_slot != HdfsScanNode::SKIP_COLUMN)
            {
                value_slot_desc.push_back(materailized_slots[index_slot]);
            }
            else
            {
                value_slot_desc.push_back(NULL);
            }
        }
    }
    kv_parser_->Set_Key_State(key_types,key_slot_desc,stream_->compact_data());
    kv_parser_->Set_Value_State(value_types,value_slot_desc,stream_->compact_data());

}
Status HdfsHFileScanner::ProcessTrailer(bool* eosr)
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

//    DCHECK_GE(len, FixedFileTrailer::MAX_TRAILER_SIZE);

    RETURN_IF_ERROR(hfile::FixedFileTrailer::SetDataFromBuffer(buffer, len, *trailer_));

    if(scan_node_->materialized_slots().empty())
    {
        //no materialized columns. We can server this query from just the metadata.
        //We don't need to read the column data.

        int64_t num_tuples = trailer_->entry_count_;
        COUNTER_UPDATE(scan_node_->rows_read_counter(), num_tuples);

        MemPool* pool;
        Tuple* tuple;
        TupleRow* current_row;
        while(num_tuples > 0)
        {
            int max_tuples = GetMemory(&pool, &tuple,&current_row);
            max_tuples = min(static_cast<int64_t>(max_tuples), num_tuples);
            num_tuples -= max_tuples;

            int num_to_commit = WriteEmptyTuples(context_, current_row,
                                                 max_tuples);
            if (num_to_commit > 0)
                CommitRows(num_to_commit);
        }
        *eosr = true;
    }
    return Status::OK;
}



Status HdfsHFileScanner::ProcessSplitInternal()
{
    DCHECK(scan_node_ ->materialized_slots().size()!=0);
    while(!scan_node_->ReachedLimit() && !context_->cancelled())
    {
        MemPool* pool;
        Tuple* tuple;
        TupleRow* row;
        int num_rows = GetMemory(&pool, &tuple, &row);
        int num_to_commit = 0;

        for(int i = 0; i < num_rows; i++)
        {
            InitTuple(template_tuple_, tuple);
            if(!WriteTuple(pool, tuple))
            {
                CommitRows(num_to_commit);

                return parse_status_;
            }
            row->SetTuple(scan_node_->tuple_idx(), tuple);
            if(ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, row))
            {
                row = next_row(row);
                tuple = next_tuple(tuple);
                ++num_to_commit;
            }
        }
        if(num_to_commit > 0)
            CommitRows(num_to_commit);
    }

    if(context_->cancelled())
        return Status::CANCELLED;
    return parse_status_;

}

Status HdfsHFileScanner::Close()
{
    context_->Close();
    // not uniformly compressed.
    if(!only_parsing_trailer_)
        scan_node_->RangeComplete(THdfsFileFormat::HFILE, THdfsCompression::NONE);
//  assemble_rows_timer_.UpdateCounter();
    return Status::OK;
}

Status HdfsHFileScanner::Prepare(ScannerContext* context)
{
    RETURN_IF_ERROR(HdfsScanner::Prepare(context));
    const TupleDescriptor* tuple_desc = scan_node_->tuple_desc();
    const HdfsTableDescriptor* hdfs_table = static_cast<const HdfsTableDescriptor*>(tuple_desc->table_desc());
    col_types_ = hdfs_table->col_types();
    col_names_ = hdfs_table->col_names();
    key_col_names_ = hdfs_table->key_col_names();
    num_clustering_cols_ = hdfs_table->num_clustering_cols();
//    scan_node_->IncNumScannersCodegenDisabled();
    decompress_timer_ = ADD_TIMER(scan_node_->runtime_profile(), "DecompressionTime");

    return Status::OK;
}

bool HdfsHFileScanner::WriteTuple(MemPool * pool, Tuple * tuple)
{
    if(byte_buffer_ptr_ == byte_buffer_end_)
    {
        parse_status_= ReadDataBlock();
        if(byte_buffer_ptr_ == byte_buffer_end_)
            return false;
    }
    bool ret = kv_parser_->Write_Tuple(pool,tuple,&byte_buffer_ptr_);
    return ret;
}


Status HdfsHFileScanner::ReadDataBlock()
{
    Status status;
    uint8_t* buffer;
    int num_bytes;
    bool eos;

    if(!stream_->compact_data())
    {
        AttachPool(decompressed_data_pool_.get());
        block_buffer_len_ = 0;
    }

    //need to read in a loop to skip no-data-block-type block
    while(true)
    {

        if(!read_block_header_)
        {
            //this is the first time reading data block, or just skip a no data block.
            if(!stream_->GetBytes(trailer_->header_size_, &buffer, &num_bytes, &eos, &status))
                return status;
            DCHECK_EQ(trailer_->header_size_, num_bytes);
            Read_Header(buffer, trailer_->minor_version_);
            read_block_header_ = true;
        }
        //it is suffice to  compare prefix to determine whether this block is a data block
        if(memcmp(block_header_.block_type_,FixedFileTrailer::DATA_BLOCK_TYPE,7))
        {
            if(stream_->file_offset() >  trailer_->last_data_block_offset_)

                //has already read all data blocks, mission complete.
                break;
            int64_t previous_offset = stream_->file_offset();
            //this block is not a data block, skip this block and continue;
            if(!stream_->SkipBytes(block_header_.on_disk_size_without_header_, &status))
            {
                stringstream ss;
                ss<<status.GetErrorMsg()<<",current file offset:"<<stream_->file_offset()<<",offset before skip :"<<previous_offset<<" ,last_data_block_offset_"<<trailer_->last_data_block_offset_
                  <<",on_disk_size_without_header_:"<<block_header_.on_disk_size_without_header_<<",block type"
                  <<string(reinterpret_cast<char*>(block_header_.block_type_),8);
                return Status(ss.str());
            }

            read_block_header_ = false;
            continue;
        }
        //it's really a data block, read it, plus next block's header.
        if(!stream_->ReadBytes(block_header_.on_disk_size_without_header_ + trailer_->header_size_, &buffer, &status))
        {
            return status;
        }
        //trailer_.compression_codec_ is ordinal value of compression enum.
        // no compression
        uint8_t* data_buffer_ptr;
        if(trailer_->compression_codec_ == 2)
        {
            DCHECK_EQ(block_header_.on_disk_size_without_header_, block_header_.uncompressed_size_without_header_+block_header_.checksum_size_);
            data_buffer_ptr = buffer;
        }
        else if(trailer_->compression_codec_ == 3) //snappy compression.
        {
            SCOPED_TIMER(decompress_timer_);
            if(block_buffer_len_ < block_header_.uncompressed_size_without_header_)
            {
                block_buffer_len_ = block_header_.uncompressed_size_without_header_;
                block_buffer_ = decompressed_data_pool_->Allocate(block_buffer_len_);
            }
            RETURN_IF_ERROR(decompressor_->ProcessBlock(static_cast<int>(block_header_.on_disk_data_size_without_header_),
                            buffer,reinterpret_cast<int*>(&block_header_.uncompressed_size_without_header_),&block_buffer_));
            data_buffer_ptr = block_buffer_;
        }
        else
        {
            DCHECK(false);//currently only support snappy compression.
        }
        byte_buffer_ptr_ = data_buffer_ptr;
        byte_buffer_end_ = data_buffer_ptr +block_header_.uncompressed_size_without_header_;

        uint8_t*  byte_next_header_ptr = buffer + block_header_.on_disk_size_without_header_;

        Read_Header(byte_next_header_ptr, trailer_->minor_version_);
        //has found a data block, break out of the loop
        break;
    }

    return Status::OK;
}


//put scan range into queue, which will be issued to io manager when scanner thread terminate.
Status HdfsHFileScanner::IssueFileRanges(const char* filename)
{

    HdfsFileDesc* file_desc = scan_node_->GetFileDesc(filename);

    vector<DiskIoMgr::ScanRange*> ranges;
    ScanRangeMetadata* metadata =
        reinterpret_cast<ScanRangeMetadata*>(file_desc->splits[0]->meta_data());

    DiskIoMgr::ScanRange* range = scan_node_->AllocateScanRange(filename, file_desc->file_length,
                                  trailer_->first_data_block_offset_, metadata->partition_id, -1);
    ranges.push_back(range);
    scan_node_->AddDiskIoRanges(ranges);

    return Status::OK;

}

void HdfsHFileScanner::IssueInitialRanges(HdfsScanNode* scan_node,
        const std::vector<HdfsFileDesc*>& files)
{
    vector<DiskIoMgr::ScanRange*> ranges;
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
	      ranges.push_back(footer_range);
        }
    }
    scan_node->AddDiskIoRanges(ranges);

}


