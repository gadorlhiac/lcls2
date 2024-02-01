#include "HREncoder.hh"

#include "xtcdata/xtc/Array.hh"
#include "xtcdata/xtc/VarDef.hh"
#include "xtcdata/xtc/DescData.hh"
#include "xtcdata/xtc/NamesLookup.hh"
#include "DataDriver.h"
#include "psalg/utils/SysLog.hh"

#include <Python.h>
#include <stdint.h>
#include <stdio.h>
#include <assert.h>
#include <fstream>
#include <string>
#include <iostream>

using namespace XtcData;
using logging = psalg::SysLog;
using json = nlohmann::json;

namespace Drp {

  namespace Enc {
    class RawStream {
    public:
        static void varDef(VarDef& v,unsigned ch) {
            char name[32];
            // raw streams
            sprintf(name,"raw_%d",ch);
            v.NameVec.push_back(XtcData::Name(name, XtcData::Name::UINT64,1));
        }
        static void createData(CreateData& cd, unsigned& index, unsigned ch, Array<uint8_t>& seg) {
            unsigned shape[MaxRank];
            shape[0] = seg.shape()[0]>>1;
            Array<uint64_t> arrayT = cd.allocate<uint64_t>(index++, shape);
            memcpy(arrayT.data(), seg.data(), seg.shape()[0]);
        }
    };
    class ProcStream {
    public:
        static void varDef(VarDef& v) {
	         //v.NameVec.push_back(XtcData::Name("encoder", XtcData::Name::UINT64));
            v.NameVec.push_back(XtcData::Name("position", XtcData::Name::UINT32));
            v.NameVec.push_back(XtcData::Name("error_cnt", XtcData::Name::UINT8));
            v.NameVec.push_back(XtcData::Name("missedTrig_cnt", XtcData::Name::UINT8));
	          v.NameVec.push_back(XtcData::Name("latches", XtcData::Name::UINT8));
            //v.NameVec.push_back(XtcData::Name("Elatch", XtcData::Name::UINT8)); // 1 bit
            //v.NameVec.push_back(XtcData::Name("Platch", XtcData::Name::UINT8)); // 1 bit
            //v.NameVec.push_back(XtcData::Name("Qlatch", XtcData::Name::UINT8)); // 1 bit
        }

        static void createData(CreateData& cd, unsigned& index, Array<uint8_t>& seg) {
            ProcStream& p = *new (seg.data()) ProcStream;
      	    //cd.set_value(index++, p.m_output);

            cd.set_value(index++, p.m_position);
            cd.set_value(index++, p.m_encErrCnt);
            cd.set_value(index++, p.m_missedTrigCnt);
	          cd.set_value(index++, p.m_latches);// & 0b11100000); //0xe0

	          p._dump();
        }
	ProcStream(){}
    private:
        void _dump() const
        {
	          std::cout << "Position: " << m_position << std::endl;
	          std::cout << "Error Count: " << m_encErrCnt << std::endl;
	          std::cout << "Missed Triggers: " << m_missedTrigCnt << std::endl;
	          std::cout << "Latch bits: " << m_latches << std::endl;
	          std::cout << "Reserved: " << m_reserved << std::endl;
	          // std::cout << "Full value: " << m_output << std::endl;
        }

        //uint64_t m_output {};
        uint32_t m_position {};
        uint8_t m_encErrCnt {};
        uint8_t m_missedTrigCnt {};
        uint8_t m_latches {}; // upper 3 bits define 3 latch bits
        uint8_t m_reserved {}; // actually 13 bits

    };
    class Streams {
    public:
        static void defineData(Xtc& xtc, const void* bufEnd, const char* detName,
                               const char* detType, const char* detNum,
                               NamesLookup& lookup, NamesId& raw, NamesId& fex) {
          // set up the names for L1Accept data
          { Alg alg("raw", 0, 0, 1);
            Names& eventNames = *new(xtc, bufEnd) Names(bufEnd,
                                                        detName, alg,
                                                        detType, detNum, raw);
            VarDef v;
            ProcStream::varDef(v);
            eventNames.add(xtc, bufEnd, v);
            lookup[raw] = NameIndex(eventNames); }
          /*{ Alg alg("fex", 0, 0, 1);
            Names& eventNames = *new(xtc, bufEnd) Names(bufEnd,
                                                        detName, alg,
                                                        detType, detNum, fex);
            VarDef v;
            eventNames.add(xtc, bufEnd, v);
            lookup[fex] = NameIndex(eventNames); }*/
        }
        static void createData(XtcData::Xtc&         xtc,
                               const void*           bufEnd,
                               XtcData::NamesLookup& lookup,
                               XtcData::NamesId&     rawId,
                               XtcData::NamesId&     fexId,
                               XtcData::Array<uint8_t>* streams) {
	          std::cout  << "Streams::createData" << std::endl;
	          CreateData raw(xtc, bufEnd, lookup, rawId);
            std::cout << "after raw" << std::endl;

	          unsigned index = 0;
	          ProcStream::createData(raw, index, *streams);
	          std::cout << "After ProcStream::createData" << std::endl;
	          index = 0;
            //CreateData fex(xtc, bufEnd, lookup, fexId);

       }
    }; 

  } // Enc

HREncoder::HREncoder(Parameters* para, MemPool* pool) :
    BEBDetector(para, pool)
{
    logging::info("Before _init");
    _init(para->kwargs["epics_prefix"].c_str());
    logging::info("After _init");

    if (para->kwargs.find("timebase")!=para->kwargs.end() &&
        para->kwargs["timebase"]==std::string("119M"))
        m_debatch = true;
}

unsigned HREncoder::_configure(Xtc& xtc, const void* bufEnd, ConfigIter&)
{
    // set up the names for the event data
    m_evtNamesRaw = NamesId(nodeId, EventNamesIndex+0);
    m_evtNamesFex = NamesId(nodeId, EventNamesIndex+1);
    Enc::Streams::defineData(xtc,bufEnd,m_para->detName.c_str(),
                            m_para->detType.c_str(),m_para->serNo.c_str(),
                            m_namesLookup,m_evtNamesRaw,m_evtNamesFex);
    return 0;
}

void HREncoder::connect(const json& connect_json, const std::string& collectionId)
{
    logging::info("HREncoder_connect");
}
void HREncoder::_event(XtcData::Xtc& xtc,
                   const void* bufEnd,
                   std::vector< XtcData::Array<uint8_t> >& subframes)
{
    std::cout << "HREncoder::_event" << std::endl;
    std::cout << "Num subframes: " << subframes.size() << std::endl; // 4
    std::cout << "Num elems sf[0]: " << subframes[0].num_elem() << std::endl;
    std::cout << "Num elems sf[1]: " << subframes[1].num_elem() << std::endl;
    std::cout << "Num elems sf[2]: " << subframes[2].num_elem() << std::endl;
    std::cout << "Num elems sf[3]: " << subframes[3].num_elem() << std::endl;

    Enc::Streams::createData(xtc, bufEnd, m_namesLookup, m_evtNamesRaw, m_evtNamesFex, &subframes[2]);
}
} // Drp
