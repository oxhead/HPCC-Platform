/*##############################################################################

    HPCC SYSTEMS software Copyright (C) 2012 HPCC Systems®.

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

#ifdef _WIN32
#pragma warning( disable : 4786)
#pragma warning( disable : 4018)
#endif

#include "platform.h"
#undef new
#include <string>
#include <map>
#include <queue>

#include "jthread.hpp"
#include "jlog.hpp"
#include "jisem.hpp"
#include "udplib.hpp"
#include "udptrr.hpp"
#include "udptrs.hpp"
#include "roxiemem.hpp"

using roxiemem::DataBuffer;
using roxiemem::IRowManager;

#if defined(_DEBUG) && defined(_WIN32) && !defined(USING_MPATROL)
 #define new new(_NORMAL_BLOCK, __FILE__, __LINE__)
#endif

bool streamingSupported = false;

atomic_t unwantedDiscarded;
atomic_t packetsRetried;
atomic_t packetsAbandoned;

// PackageSequencer ====================================================================================
//
typedef DataBuffer * data_buffer_ptr;

#ifndef _STACKTRACE_H_
#define _STACKTRACE_H_

#include <stdio.h>
#include <stdlib.h>
#include <execinfo.h>
#include <cxxabi.h>

/** Print a demangled stack backtrace of the caller function to FILE* out. */
static inline void print_stacktrace(unsigned int max_frames = 63)
{
	DBGLOG("stack trace:");

	// storage array for stack trace address data
	void* addrlist[max_frames + 1];

	// retrieve current stack addresses
	int addrlen = backtrace(addrlist, sizeof(addrlist) / sizeof(void*));

	if (addrlen == 0) {
		DBGLOG("  <empty, possibly corrupt>");
		return;
	}

	// resolve addresses into strings containing "filename(function+address)",
	// this array must be free()-ed
	char** symbollist = backtrace_symbols(addrlist, addrlen);

	// allocate string which will be filled with the demangled function name
	size_t funcnamesize = 256;
	char* funcname = (char*)malloc(funcnamesize);

	// iterate over the returned symbol lines. skip the first, it is the
	// address of this function.
	for (int i = 1; i < addrlen; i++)
	{
		char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

		// find parentheses and +address offset surrounding the mangled name:
		// ./module(function+0x15c) [0x8048a6d]
		for (char *p = symbollist[i]; *p; ++p)
		{
			if (*p == '(')
				begin_name = p;
			else if (*p == '+')
				begin_offset = p;
			else if (*p == ')' && begin_offset) {
				end_offset = p;
				break;
			}
		}

		if (begin_name && begin_offset && end_offset
			&& begin_name < begin_offset)
		{
			*begin_name++ = '\0';
			*begin_offset++ = '\0';
			*end_offset = '\0';

			// mangled name is now in [begin_name, begin_offset) and caller
			// offset in [begin_offset, end_offset). now apply
			// __cxa_demangle():

			int status;
			char* ret = abi::__cxa_demangle(begin_name,
				funcname, &funcnamesize, &status);
			if (status == 0) {
				funcname = ret; // use possibly realloc()-ed string
				DBGLOG("  %s : %s+%s",
					symbollist[i], funcname, begin_offset);
			}
			else {
				// demangling failed. Output function name as a C function with
				// no arguments.
				DBGLOG("  %s : %s()+%s",
					symbollist[i], begin_name, begin_offset);
			}
		}
		else
		{
			// couldn't parse the line? print the whole line.
			DBGLOG("  %s", symbollist[i]);
		}
	}

	free(funcname);
	free(symbollist);
}

#endif // _STACKTRACE_H_

int g_sequence_compare(const void *arg1, const void *arg2 ) 
{
    DataBuffer *dataBuff1 =  *(DataBuffer **) arg1;
    DataBuffer *dataBuff2 =  *(DataBuffer **) arg2;
    UdpPacketHeader *pktHdr1 = (UdpPacketHeader*) dataBuff1->data;
    UdpPacketHeader *pktHdr2 = (UdpPacketHeader*) dataBuff2->data;
    if (pktHdr1->pktSeq < pktHdr2->pktSeq) return -1;
    if (pktHdr1->pktSeq > pktHdr2->pktSeq) return 1;
    return 0;
}


class PackageSequencer : public CInterface, implements IInterface
{
    DataBuffer *firstPacket;
    DataBuffer *lastContiguousPacket;
    unsigned metaSize;
    unsigned headerSize;
    const void *header;

    MemoryBuffer metadata;
    InterruptableSemaphore dataAvailable; // MORE - need to work out when to interrupt it!
    SpinLock streamLock; // Needed if streaming is supported since data can be read as blocks are being written. MORE - not 100% sure it is needed as semaphore probably protects adequately.

public:
    IMPLEMENT_IINTERFACE;

    PackageSequencer() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: PackageSequencer::PackageSequencer this=%p", this);
        metaSize = 0;
        headerSize = 0;
        header = NULL;
        firstPacket = NULL;
        lastContiguousPacket = NULL;
    }

    ~PackageSequencer() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: PackageSequencer::~PackageSequencer this=%p", this);
        DataBuffer *finger = firstPacket;
        while (finger)
        {
            DataBuffer *goer = finger;
            finger = finger->msgNext;
            goer->Release();
        }
    }
    
    DataBuffer *next(DataBuffer *after)
    {
        dataAvailable.wait(); // MORE - when do I interrupt? Should I time out? Will potentially block indefinately if sender restarts (leading to an abandoned packet) or stalls.
        DataBuffer *ret;
        {
            SpinBlock b(streamLock);
            if (after)
                ret = after->msgNext; 
            else
                ret = firstPacket;
        }
        if (checkTraceLevel(TRACE_MSGPACK, 5))
        {
            if (ret)
            {
                UdpPacketHeader *pktHdr = (UdpPacketHeader*) ret->data;
                DBGLOG("UdpCollator: PackageSequencer::next returns ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%u dataBuffer=%p this=%p", 
                        pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->nodeIndex, ret, this);
            }
            else
                DBGLOG("UdpCollator: PackageSequencer::next returns NULL this=%p", this);
        }
        return ret;
    }

    bool insert(DataBuffer *dataBuff)  // returns true if message is complete.
    {
        bool res = false;
        assert(dataBuff->msgNext == NULL);
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;

        if (checkTraceLevel(TRACE_MSGPACK, 5))
        {
            DBGLOG("UdpCollator: PackageSequencer::insert ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%u dataBuffer=%p this=%p", 
                    pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->nodeIndex, dataBuff, this);
        }

        DataBuffer *finger;
        DataBuffer *prev;
        SpinBlock b(streamLock);
        if (lastContiguousPacket)
        {
            UdpPacketHeader *oldHdr = (UdpPacketHeader*) lastContiguousPacket->data;
            if (pktHdr->pktSeq <= oldHdr->pktSeq)
            {
                // discard duplicated incoming packet - should be uncommon unless we requested a resend for a packet already in flight
                if (checkTraceLevel(TRACE_MSGPACK, 5))
                    DBGLOG("UdpCollator: Discarding duplicate incoming packet");
                dataBuff->Release();
                return false;
            }
            finger = lastContiguousPacket->msgNext;
            prev = lastContiguousPacket;
        }
        else
        {
            finger = firstPacket; 
            prev = NULL;
        }
        while (finger)
        {
            UdpPacketHeader *oldHdr = (UdpPacketHeader*) finger->data;
            if (pktHdr->pktSeq == oldHdr->pktSeq)
            {
                // discard duplicated incoming packet - should be uncommon unless we requested a resend for a packet already in flight
                if (checkTraceLevel(TRACE_MSGPACK, 5))
                    DBGLOG("UdpCollator: Discarding duplicate incoming packet");
                dataBuff->Release();
                return false;
            }
            else if (pktHdr->pktSeq < oldHdr->pktSeq)
            {
                break;
            }
            else
            {
                prev = finger;
                finger = finger->msgNext;
            }
        }
        if (prev)
        {
            assert(prev->msgNext == finger);
            prev->msgNext = dataBuff;
        }
        else
            firstPacket = dataBuff;
        dataBuff->msgNext = finger;
        if (prev == lastContiguousPacket)
        {
            unsigned prevseq;
            if (lastContiguousPacket)
            {
                prevseq = ((UdpPacketHeader*) lastContiguousPacket->data)->pktSeq & 0x3fffffff;
                finger = lastContiguousPacket->msgNext;
            }
            else
            {
                prevseq = (unsigned) -1;
                finger = firstPacket;
            }
            while (finger)
            {
                UdpPacketHeader *fingerHdr  = (UdpPacketHeader*) finger->data;
                unsigned pktseq = fingerHdr->pktSeq & 0x3fffffff;
                if (pktseq == prevseq+1)
                {
                    unsigned packetDataSize = fingerHdr->length - fingerHdr->metalength - sizeof(UdpPacketHeader);
                    assert(packetDataSize < roxiemem::DATA_ALIGNMENT_SIZE);
                    if ((fingerHdr->pktSeq & ~0x80000000) == 0)
                    {
                        // MORE - Is this safe - header lifetime is somewhat unpredictable without a copy of it...
                        // Client header is at the start of packet 0
                        headerSize = *(unsigned short *)(finger->data + sizeof(UdpPacketHeader));
                        header = finger->data + sizeof(UdpPacketHeader) + sizeof(unsigned short);
                        packetDataSize -= headerSize + sizeof(unsigned short);
                    }
                    if (fingerHdr->metalength)
                    {
                        // MORE - may be worth taking the effort to avoid copy of metadata unless it's split up
                        metaSize += fingerHdr->metalength;
                        metadata.append(fingerHdr->metalength, finger->data + fingerHdr->length - fingerHdr->metalength);
                    }

                    lastContiguousPacket = finger;
                    dataAvailable.signal();
                    if (fingerHdr->pktSeq >= 0x80000000)
                    {
                        res = true;
                        dataAvailable.signal(); // allowing us to read the NULL that signifies end of message. May prefer to use the flag to stop?
                    }
                }
                else
                    break;
                finger = finger->msgNext;
                prevseq = pktseq;
            }
        }
        return res;
    }

    inline const void *getMetaData(unsigned &length)
    {
        length = metadata.length();
        return metadata.toByteArray();
    }

    inline const void *getMessageHeader()
    {
        return header;
    }

    inline unsigned getHeaderSize()
    {
        return headerSize;
    }

};

typedef std::queue<PackageSequencer*> seq_map_que;
typedef std::queue<void*> ptr_que;


typedef unsigned __int64 PUID;
typedef MapXToMyClass<PUID, PUID, PackageSequencer> msg_map;


// MessageResult ====================================================================================
//
class CMessageUnpackCursor: public CInterface, implements IMessageUnpackCursor
{
    PackageSequencer *pkSequencer;
    DataBuffer *dataBuff;
    unsigned current_pos;
    Linked<IRowManager> rowMgr;

public:
    IMPLEMENT_IINTERFACE;

    CMessageUnpackCursor(PackageSequencer *_pkSqncr, IRowManager *_rowMgr) : rowMgr(_rowMgr)
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageUnpackCursor::CMessageUnpackCursor this=%p", this);
        pkSequencer = _pkSqncr; 
        dataBuff = pkSequencer->next(NULL);
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        current_pos = sizeof(UdpPacketHeader) + pkSequencer->getHeaderSize() + sizeof(unsigned short); // YUK - code neads cleaning!
        unsigned packetDataLimit = pktHdr->length - pktHdr->metalength;
        while (current_pos >= packetDataLimit)
        {
            current_pos = sizeof(UdpPacketHeader);
            dataBuff = pkSequencer->next(dataBuff); // NULL, we expect, unless packing was really weird.
            if (dataBuff)
            {
                pktHdr = (UdpPacketHeader*) dataBuff->data;
                packetDataLimit = pktHdr->length - pktHdr->metalength;
            }
            else
                break;
        }
    }
    
    ~CMessageUnpackCursor() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageUnpackCursor::~CMessageUnpackCursor this=%p", this);
        pkSequencer->Release();
    }

    virtual bool atEOF() const
    {
        return dataBuff == NULL;
    }

    virtual bool isSerialized() const
    {
        return true;
    }

    virtual const void *getNext(int length) 
    {
        // YUK horrid code! Though packer is even more horrid
        void        *res = 0;
        if (dataBuff) 
        {   
            UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
            if (checkTraceLevel(TRACE_MSGPACK, 4))
            {
                StringBuffer s;
                DBGLOG("UdpCollator: CMessageUnpackCursor::getNext(%u) pos=%u pktLength=%u metaLen=%u ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%u dataBuff=%p this=%p", 
                    length, current_pos, pktHdr->length, pktHdr->metalength,
                    pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, 
                    pktHdr->nodeIndex, dataBuff, this);
            }
            unsigned packetDataLimit = pktHdr->length - pktHdr->metalength;
            if ((packetDataLimit  - current_pos) >= length) 
            {
                // Simple case - no need to copy
                res = &dataBuff->data[current_pos];
                current_pos += length;
                while (current_pos >= packetDataLimit)
                {
                    dataBuff = pkSequencer->next(dataBuff);
                    current_pos = sizeof(UdpPacketHeader);
                    if (dataBuff)
                    {
                        pktHdr = (UdpPacketHeader*) dataBuff->data;
                        packetDataLimit = pktHdr->length - pktHdr->metalength;
                    }
                    else
                        break;
                }
                LinkRoxieRow(res);
                return res;
            }   
            char *currResLoc = (char*)rowMgr->allocate(length, 0);
            res = currResLoc;
            while (length && dataBuff) 
            {
                // Spans more than one block - allocate and copy
                assert(dataBuff);
                unsigned cpyLen = packetDataLimit - current_pos;
                if (cpyLen > length) cpyLen = length;
                memcpy(currResLoc, &dataBuff->data[current_pos], cpyLen);
                length -= cpyLen;
                currResLoc += cpyLen;
                current_pos += cpyLen;
                while (current_pos >= packetDataLimit)
                {
                    dataBuff = pkSequencer->next(dataBuff);
                    if (dataBuff)
                    {
                        current_pos = sizeof(UdpPacketHeader);
                        pktHdr = (UdpPacketHeader*) dataBuff->data;
                        packetDataLimit = pktHdr->length - pktHdr->metalength;
                    }
                    else
                    {
                        current_pos = 0;
                        pktHdr = NULL;
                        packetDataLimit = 0;
                        break;
                    }
                }
            }
            assertex(!length);  // fail if not enough data available
        }
        else
            res = NULL;
        return res;
    }

};

 
class CMessageResult : public IMessageResult, CInterface {
    PackageSequencer    *pkSequencer;
    mutable MemoryBuffer metaInfo;
    mutable CriticalSection metaCrit;
    
public:
    IMPLEMENT_IINTERFACE;

    CMessageResult(PackageSequencer *_pkSqncr) 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageResult::CMessageResult pkSqncr=%p, this=%p", _pkSqncr, this);
        pkSequencer = _pkSqncr; 
    }
    
    ~CMessageResult() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageResult::~CMessageResult this=%p", this);
        pkSequencer->Release();
    }

    virtual IMessageUnpackCursor *getCursor(IRowManager *rowMgr) const 
    {
        return new CMessageUnpackCursor(LINK(pkSequencer), rowMgr);
    }

    virtual const void *getMessageHeader(unsigned &length) const 
    {
        length = pkSequencer->getHeaderSize();
        return pkSequencer->getMessageHeader();
    }

    virtual const void *getMessageMetadata(unsigned &length) const 
    {
        return pkSequencer->getMetaData(length);
    }

    virtual void discard() const 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 2))
            DBGLOG("UdpCollator: CMessageResult - Roxie server discarded a packet");
        atomic_inc(&unwantedDiscarded);
    }

};

 


// MessageCollator ====================================================================================
//

PUID GETPUID(DataBuffer *dataBuff)
{
    UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
    return (((PUID) pktHdr->nodeIndex) << 32) | (PUID) pktHdr->msgSeq;
}

class CMessageCollator : public CInterface, implements IMessageCollator
{
    seq_map_que         queue;
    msg_map             mapping;
    bool                activity;
    bool                memLimitExceeded;
    CriticalSection     queueCrit;
    CriticalSection     mapCrit;
    InterruptableSemaphore sem;
    Linked<IRowManager> rowMgr;
    ruid_t ruid;
    unsigned totalBytesReceived;

public:
    IMPLEMENT_IINTERFACE;

    CMessageCollator(IRowManager *_rowMgr, unsigned _ruid) : rowMgr(_rowMgr), ruid(_ruid)
    {
		DBGLOG("CMessageCollator:new -> ruid=%u", _ruid);
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageCollator::CMessageCollator rowMgr=%p this=%p ruid=" RUIDF "", _rowMgr, this, ruid);
        memLimitExceeded = false;
        activity = false; // w/o it there is a race condition
        totalBytesReceived = 0;
    }

    virtual ~CMessageCollator() 
    {
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageCollator::~CMessageCollator ruid=" RUIDF ", this=%p", ruid, this);
        while (!queue.empty())
        {
            PackageSequencer *pkSqncr = queue.front();
            queue.pop();
            pkSqncr->Release();
        }
    }

    virtual ruid_t queryRUID() const
    {
        return ruid;
    }

    virtual unsigned queryBytesReceived() const
    {
		//DBGLOG("udp:CMessageCollator::queryBytesReceived");
        return totalBytesReceived; // Arguably should lock, but can't be bothered. Never going to cause an issue in practice.
    }

    virtual bool add_package(DataBuffer *dataBuff) 
    {
		DBGLOG("CMessageCollator::add_package");
        UdpPacketHeader *pktHdr = (UdpPacketHeader*) dataBuff->data;
        if (checkTraceLevel(TRACE_MSGPACK, 4))
        {
            DBGLOG("UdpCollator: CMessageCollator::add_package memLimitEx=%d ruid=" RUIDF " id=0x%.8X mseq=%u pkseq=0x%.8X node=%u udpSequence=%u rowMgr=%p this=%p", 
                memLimitExceeded, pktHdr->ruid, pktHdr->msgId, pktHdr->msgSeq, pktHdr->pktSeq, pktHdr->nodeIndex, pktHdr->udpSequence, (void*)rowMgr, this);
        }

        if (memLimitExceeded || roxiemem::memPoolExhausted()) 
        {
            DBGLOG("UdpCollator: mem limit exceeded");
            return false;
        }
        if (!dataBuff->attachToRowMgr(rowMgr)) 
        {
            memLimitExceeded = true;
            DBGLOG("UdpCollator: mem limit exceeded");
            return(false);
        }
        activity = true;
        totalBytesReceived += pktHdr->length;
		DBGLOG("CMessageCollator:add_package -> length=%u", pktHdr->length);
        PUID puid = GETPUID(dataBuff);
        // MORE - I think we leak a PackageSequencer for messages that we only receive parts of - maybe only an issue for "catchall" case
        CriticalBlock b(mapCrit);
        PackageSequencer *pkSqncr = mapping.getValue(puid);
        bool isNew = false;
        bool isComplete = false;
        if (!pkSqncr) 
        {
            pkSqncr = new PackageSequencer;
            mapping.setValue(puid, pkSqncr);
            pkSqncr->Release();
            isNew = true;
        }
        isComplete = pkSqncr->insert(dataBuff);
        if (streamingSupported ? isNew : isComplete)
        {
            queueCrit.enter();
            pkSqncr->Link();
            queue.push(pkSqncr);
			DBGLOG("@@ release sem -> signal");
            sem.signal();
            queueCrit.leave();
        }
        if (isComplete)
            mapping.remove(puid);
        return(true);
    }

	int sc = 0;
    virtual IMessageResult *getNextResult(unsigned time_out, bool &anyActivity) 
    {
		DBGLOG("CMessageCollator:getNextResult");
		//print_stacktrace();
        if (checkTraceLevel(TRACE_MSGPACK, 3))
            DBGLOG("UdpCollator: CMessageCollator::getNextResult() timeout=%.8X ruid=%u rowMgr=%p this=%p", time_out, ruid, (void*) rowMgr, this);
        
        if (memLimitExceeded) 
        {
            DBGLOG("UdpCollator: CMessageCollator::getNextResult() throwing memory limit exceeded exception - rowMgr=%p this=%p", (void*) rowMgr, this);
            throw MakeStringException(0, "memory limit exceeded");
        }
        else if (roxiemem::memPoolExhausted()) 
        { 
            DBGLOG("UdpCollator: CMessageCollator::getNextResult() throwing memory pool exhausted exception - rowMgr=%p this=%p", (void*)rowMgr, this);
            throw MakeStringException(0, "memory pool exhausted");
        }
		sc++;
		DBGLOG("[%d] CMessageCollator:getNextResult -> wait", sc);
        if (sem.wait(time_out)) 
        {
			DBGLOG("[%d] CMessageCollator:getNextResult -> get sem", sc);
            queueCrit.enter();
            PackageSequencer *pkSqncr = queue.front();
            queue.pop();
            queueCrit.leave();
            anyActivity = true;
            activity = false;
            return new CMessageResult(pkSqncr);
        }
        anyActivity = activity;
        activity = false; 
        if (!anyActivity && ruid>=RUID_FIRST && checkTraceLevel(TRACE_MSGPACK, 1)) // suppress the tracing for pings where we expect the timeout...
        {
            DBGLOG("UdpCollator: CMessageCollator::GetNextResult timeout, %d partial results",  mapping.count());
        }
        return 0;
    }

    virtual void interrupt(IException *E) {
        sem.interrupt(E);
    }
};

// ====================================================================================
//

extern IMessageCollator *createCMessageCollator(IRowManager *rowManager, ruid_t ruid)
{
    return new CMessageCollator(rowManager, ruid);
}
