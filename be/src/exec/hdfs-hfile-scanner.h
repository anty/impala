/*
 * hdfs-hfile-scanner.h
 *
 *  Created on: 2013-5-9
 *      Author: Rao
 */

#ifndef HDFS_HFILE_SCANNER_H_
#define HDFS_HFILE_SCANNER_H_

#include "exec/hdfs-scanner.h"
#include "exec/scan-node.h"
#include "exec/hdfs-scan-node.h"
#include "exec/hfile-types.h"
#include <boost/scoped_ptr.hpp>

namespace impala
{

class RuntimeState;
class MemPool;
class Status;

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
	virtual Status GetNext(RowBatch* row_batch, bool* eosr);
	virtual Status ProcessSplit(ScannerContext* context);
	virtual Status Close();

	static Status Init();

private:

//	friend class LazyBinaryDeserializer;
//	friend class BinarySortableDeserializer;
	Status ProcessTrailer();
	Status ReadDataBlock();
	Status IssueFileRanges(const char* filename);
	bool WriteTuple(MemPool* pool, Tuple* tuple);
	Status ProcessSplitInternal();

	uint8_t* byte_buffer_ptr_;
	uint8_t* byte_buffer_end_;
	uint32_t num_checksum_bytes_;


	std::vector<PrimitiveType> col_types_;
	std::vector<PrimitiveType>  key_col_types_;
	std::vector<PrimitiveType>  value_col_types_;
	//in current implementation, there is no easy to figure out the number of columns in key part of a KeyValue object.
	//So we de-serialize a record to get this number.
	int num_key_cols_;
	int num_clustering_cols_;
//	class KeyValue;
	boost::scoped_ptr<KeyValue> kv_parser;
	hfile::FixedFileTrailer* trailer_;
	bool only_parsing_trailer_;

};

} //namespace impala

#endif /* HDFS_HFILE_SCANNER_H_ */
