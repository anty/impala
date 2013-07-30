#ifndef IMPALA_UTIL_URL_NORMALIZER_H
#define IMPALA_UTIL_URL_NORMALIZER_H

#include "runtime/string-value.h"
#include "runtime/string-search.h"

namespace impala
{
class UrlNormalizer
{
public:

    //does following jobs:
    // 1.trim url
    // 2.extract domain and secondary domain if exist.
    static bool NormalizeUrl(const StringValue* url, StringValue* result);


private:
    static const StringValue 	protocol;
    static const StringValue  slash;
    static const StringValue question;
    static const StringValue hash;

    static const StringSearch protocol_search;
    static const StringSearch slash_search;
    static const StringSearch question_search;
    static const StringSearch hash_search;

};

}

#endif

