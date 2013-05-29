/*
 * hfile_types.h
 *
 *  Created on: 2013-5-20
 *      Author: Rao
 */

#ifndef HFILE_TYPES_H_
#define HFILE_TYPES_H_

#include "hfile-common.h"
#include "common/status.h"
#include <string>


namespace hfile
{

class FixedFileTrailer
{
public:
	
	static impala::Status DeserializeFromBuffer( uint8_t* buffer, int len,FixedFileTrailer& trailer);
	
	static const int MAX_COMPARATOR_NAME_LENGTH = 128;
	static const uint8_t TRAILER_BLOCK_TYPE[] ;
	static const int TRAILER_SIZE[];
	static const uint8_t DATA_BLOCK_TYPE[];
	static const uint8_t ENCODED_DATA_BLOCK_TYPE[];
	//infer from corresponding class FixedFileTrailer in HBase.
	static const int MAX_TRAILER_SIZE = 212;
	static const int MINOR_VERSION_WITH_CHECKSUM=1;
	static const int HEADER_SIZE_NO_CHECKSUM=24;
	static const int HEADER_SIZE_WITH_CHECKSUMS=HEADER_SIZE_NO_CHECKSUM+9;
	static int GetTrailerSize(int version);
	
	//offset to the file info data, a small block of vitals.
	//Necessary in v1 but only potentially useful for pretty-printing in v2
	uint64_t file_info_offset_;

	//In version 1, the offset to the data block index. Starting from version 2,
	//the meaning of this field is the offset to the section of the file that
	//should be loaded at the time the file is being opened, and as of the time
	//of writing, this happends to be the off set of the file info section
	uint64_t load_on_open_data_offset_;

	//the number of entries in the root data index
	uint32_t data_index_count_;

	//total uncompressed size of all blocks of the data index
	uint64_t uncompressed_data_index_size;

	//the number of entries in the meta index
	uint32_t meta_index_count_;

	//the total uncompressed size of keys/values stored in the file
	uint64_t total_uncompressed_bytes_;

	uint64_t entry_count_;

	//TODO

	uint32_t compression_codec_;

	//the number of levels in the potential multi-level data index. Used from version 2 onwards.
	uint32_t num_data_index_levels_;

	//the offset of the first data block
	uint64_t first_data_block_offset_;

	//it is guaranteed that no key/value data blocks start after this offset in the file
	uint64_t last_data_block_offset_;

	//row key comparator class name in version 2
	std::string comparator_class_name_;

	//the hfile format major version
	uint32_t major_version_;

	//the hfile format minor version
	uint32_t minor_version_;


	//the size of the header
	uint32_t header_size_;

};

}

#endif /* HFILE_TYPES_H_ */
