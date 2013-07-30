#include "util/url-normalizer.h"
#include "runtime/string-value.inline.h"

using namespace std;

namespace impala
{
const StringValue UrlNormalizer::protocol(const_cast<char*>("://"), 3);
const StringValue UrlNormalizer::slash(const_cast<char*>("/"), 1);
const StringValue UrlNormalizer::question(const_cast<char*>("?"), 1);
const StringValue UrlNormalizer::hash(const_cast<char*>("#"), 1);

const StringSearch UrlNormalizer::protocol_search(&protocol);
const StringSearch UrlNormalizer::slash_search(&slash);
const StringSearch UrlNormalizer::question_search(&question);
const StringSearch UrlNormalizer::hash_search(&hash);


bool UrlNormalizer::NormalizeUrl(const StringValue* url, StringValue* result)

{
    result->ptr = NULL;
    result->len = 0;
    // Remove leading and trailing spaces.
    StringValue trimmed_url = url->Trim();
    // All parts require checking for the protocol.
    int32_t protocol_pos = protocol_search.Search(&trimmed_url);
    if (protocol_pos < 0) return false;
    // Positioned to first char after '://'.
    StringValue protocol_end = trimmed_url.Substring(protocol_pos + protocol.len);
    // Find first '/'.
    const int32_t start_pos = slash_search.Search(&protocol_end);
    if (start_pos < 0)    // https://www.google.com
    {
        *result = protocol_end;
        return true;
    }

    StringValue path_start = protocol_end.Substring(start_pos+1);
    int32_t end_pos = slash_search.Search(&path_start);

    if(end_pos < 0)
    {
        *result = protocol_end;
        return true;
    }
    *result = protocol_end.Substring(0,end_pos + start_pos+1);
    return true;


//
//
//    //frame path component.
//    int32_t  path_end_pos = question_search.Search(&path_start);
//    if(path_end_pos < 0)
//    {
//        // No '?' was found, look for '#'.
//        path_end_pos = hash_search.Search(&path_start);
//    }
//    path_start = path_start.Substring(0,path_end_pos);
//    int32_t end_pos = slash_search.Search(&path_start);
//    if(end_pos < 0)
//    {
//        end_pos = path_start.len;
//    }
//    *result = protocol_end.Substring(0,end_pos + start_pos+1);
//    return true;


//  -- version 1--------------------
//  -- requirement as per my understanding, conflict with real requirement.
//	result->ptr = NULL;
//    result->len = 0;
//    // Remove leading and trailing spaces.
//    StringValue trimmed_url = url->Trim();
//    // All parts require checking for the protocol.
//    int32_t protocol_pos = protocol_search.Search(&trimmed_url);
//    if (protocol_pos < 0) return false;
//    // Positioned to first char after '://'.
//    StringValue protocol_end = trimmed_url.Substring(protocol_pos + protocol.len);
//    // Find first '/'.
//    const int32_t start_pos = slash_search.Search(&protocol_end);
//    if (start_pos < 0)    // https://www.google.com
//    {
//        *result = protocol_end;
//        return true;
//    }
//    StringValue path_start = protocol_end.Substring(start_pos+1);
//    //https://www.google.com/
//    if(path_start.len == 0)
//    {
//	*result = protocol_end.Substring(0,start_pos);
//	return true;
//    }
//    //frame path component.
//    int32_t  path_end_pos = question_search.Search(&path_start);
//    if(path_end_pos < 0)
//    {
//        // No '?' was found, look for '#'.
//        path_end_pos = hash_search.Search(&path_start);
//    }
//    path_start = path_start.Substring(0,path_end_pos);
//    int32_t end_pos = slash_search.Search(&path_start);
//    if(end_pos < 0)
//    {
//        end_pos = path_start.len;
//    }
//    *result = protocol_end.Substring(0,end_pos + start_pos+1);
//    return true;
}
}

