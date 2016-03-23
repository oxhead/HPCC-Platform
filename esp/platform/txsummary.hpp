/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2016 HPCC Systems®.

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
############################################################################## */

#ifndef TXSUMMARY_HPP
#define TXSUMMARY_HPP

#include "jiface.hpp"
#include "tokenserialization.hpp"
#include "esphttp.hpp"
#include <list>

class CTxSummary : extends CInterface
{
public:
    IMPLEMENT_IINTERFACE;
    
    // Construct an instance with the given creation time. A non-zero value
    // allows the summary to be in sync with its owning object. A value of
    // zero causes the summary to base its elapsed time calculations on its
    // own construction time.
    CTxSummary(unsigned creationTime = 0);

    // Returns the number of summary entries.
    virtual unsigned __int64 size() const;

    // Purges all summary entries.
    virtual void clear();

    // Returns true if an entry exists for the key.
    virtual bool contains(const char* key) const;

    // Returns the number of milliseconds elapsed since the construction of
    // the summary.
    virtual unsigned getElapsedTime() const;

    // Appends all summary entries to the given buffer.
    virtual void serialize(StringBuffer& buffer) const;

    // Adds the unique key and value to the end of the summary.
    // Returns true if the key value pair are added to the summary. Returns
    // false if the key is NULL, empty, or not unique within the summary.
    virtual bool append(const char* key, const char* value);
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
    bool append(const char* key, const TValue& value, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());

    // Updates the value associated with an existing key, or appends the key
    // and value to the summary if it is not already found. Returns false if
    // the key is NULL or empty. Returns true otherwise.
    virtual bool set(const char* key, const char* value);
    template <typename TValue, typename TSuffix = const char*, class TSerializer = TokenSerializer>
    bool set(const char* key, const TValue& value, const TSuffix& suffix = "", const TSerializer& serializer = TSerializer());

protected:
    // Log the summary contents on destruction.
    ~CTxSummary();

private:
    void log();

    struct Entry
    {
        StringBuffer key;
        StringBuffer value;
    };

    using Entries = std::list<Entry>;

    Entries   m_entries;
    unsigned  m_creationTime;
};


// Convenience wrapper of the default append method.
template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::append(const char* key, const TValue& value, const TSuffix& suffix, const TSerializer& serializer)
{
    auto buffer = serializer.makeBuffer();
    serializer.serialize(value, buffer);
    serializer.serialize(suffix, buffer);
    return append(key, serializer.str(buffer));
}

// Convenience wrapper of the default set method.
template <typename TValue, typename TSuffix, class TSerializer>
inline bool CTxSummary::set(const char* key, const TValue& value, const TSuffix& suffix, const TSerializer& serializer)
{
    auto buffer = serializer.makeBuffer();
    serializer.serialize(value, buffer);
    serializer.serialize(suffix, buffer);
    return set(key, serializer.str(buffer));
}

#endif // TXSUMMARY_HPP