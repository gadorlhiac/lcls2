#include "PvaDetector.hh"

#ifdef NDEBUG
#undef NDEBUG
#endif

#include <getopt.h>
#include <cassert>
#include <bitset>
#include <chrono>
#include <unistd.h>
#include <iostream>
#include <map>
#include <algorithm>
#include <limits>
#include <thread>
#include <Python.h>
#include "DataDriver.h"
#include "RunInfoDef.hh"
#include "xtcdata/xtc/Damage.hh"
#include "xtcdata/xtc/DescData.hh"
#include "xtcdata/xtc/ShapesData.hh"
#include "xtcdata/xtc/NamesLookup.hh"
#include "psdaq/service/kwargs.hh"
#include "psdaq/service/EbDgram.hh"
#include "psdaq/eb/TebContributor.hh"
#include "psalg/utils/SysLog.hh"
#include "psdaq/service/fast_monotonic_clock.hh"

#ifndef POSIX_TIME_AT_EPICS_EPOCH
#define POSIX_TIME_AT_EPICS_EPOCH 631152000u
#endif

using namespace XtcData;
using namespace Pds;
using json = nlohmann::json;
using logging = psalg::SysLog;
using ms_t = std::chrono::milliseconds;
using ns_t = std::chrono::nanoseconds;

namespace Drp {

struct PvParameters : public Parameters
{
    std::vector<std::string> pvSpecs;
};

};

static const TimeStamp TimeMax(std::numeric_limits<unsigned>::max(),
                               std::numeric_limits<unsigned>::max());
static unsigned tsMatchDegree = 2;

//
//  Put all the ugliness of non-global timestamps here
//
static int _compare(const TimeStamp& ts1,
                    const TimeStamp& ts2) {
  int result = 0;

  if ((tsMatchDegree == 0) && !(ts2 == TimeMax))
      return result;

  if (tsMatchDegree == 1) {
    /*
    **  Mask out the fiducial
    */
    const uint64_t mask = 0xfffffffffffe0000ULL;
    uint64_t ts1m = ts1.value()&mask;
    uint64_t ts2m = ts2.value()&mask;

    const uint64_t delta = 10000000; // 10 ms!
    if      (ts1m > ts2m)  result = ts1m - ts2m > delta ?  1 : 0;
    else if (ts2m > ts1m)  result = ts2m - ts1m > delta ? -1 : 0;

    return result;
  }

  if      (ts1 > ts2) result = 1;
  else if (ts2 > ts1) result = -1;
  return result;
}

template<typename T>
static int64_t _deltaT(const TimeStamp& ts)
{
    auto now = std::chrono::system_clock::now();
    auto tns = std::chrono::seconds{ts.seconds() + POSIX_TIME_AT_EPICS_EPOCH}
             + std::chrono::nanoseconds{ts.nanoseconds()};
    std::chrono::system_clock::time_point tp{std::chrono::duration_cast<std::chrono::system_clock::duration>(tns)};
    return std::chrono::duration_cast<T>(now - tp).count();
}

namespace Drp {

static const Name::DataType xtype[] = {
  Name::UINT8 , // pvBoolean
  Name::INT8  , // pvByte
  Name::INT16 , // pvShort
  Name::INT32 , // pvInt
  Name::INT64 , // pvLong
  Name::UINT8 , // pvUByte
  Name::UINT16, // pvUShort
  Name::UINT32, // pvUInt
  Name::UINT64, // pvULong
  Name::FLOAT , // pvFloat
  Name::DOUBLE, // pvDouble
  Name::CHARSTR, // pvString
};

struct DataDsc {
    uint32_t shape[MaxRank];
    void*    data;
};

class RawDef : public VarDef
{
public:
    enum index { field };
    RawDef(std::string& field, Name::DataType dType, int rank)
    {
        NameVec.push_back({field.c_str(), dType, rank});
    }
};

class InfoDef : public VarDef
{
public:
    enum index { keys, detName };
    InfoDef(std::string& detName)
    {
        NameVec.push_back({"keys",          Name::CHARSTR, 1});
        NameVec.push_back({detName.c_str(), Name::CHARSTR, 1});
    }
};

PvMonitor::PvMonitor(const PvParameters& para,
                     const std::string&  alias,
                     const std::string&  pvName,
                     const std::string&  provider,
                     const std::string&  request,
                     const std::string&  field,
                     unsigned            id,
                     size_t              nBuffers,
                     uint32_t            firstDim) :
    Pds_Epics::PvMonitorBase(pvName, provider, request, field),
    m_para                  (para),
    m_state                 (NotReady),
    m_id                    (id),
    m_firstDimOverride      (firstDim),
    m_alias                 (alias),
    pvQueue                 (nBuffers),
    bufferFreelist          (pvQueue.size()),
    m_notifySocket          {&m_context, ZMQ_PUSH},
    m_nUpdates              (0),
    m_nMissed               (0)
{
    // ZMQ socket for reporting errors
    m_notifySocket.connect({"tcp://" + m_para.collectionHost + ":" + std::to_string(CollectionApp::zmq_base_port + m_para.partition)});
}

int PvMonitor::getParams(std::string& fieldName,
                         Name::DataType& xtcType,
                         int& rank)
{
    std::unique_lock<std::mutex> lock(m_mutex);

    if (m_state == NotReady) {
        if (this->Pds_Epics::PvMonitorBase::getParams(m_type, m_nelem, m_rank))  {
            const std::chrono::seconds tmo(3);
            m_condition.wait_for(lock, tmo, [this] { return m_state == Armed; });
            if (m_state != Armed) {
                auto msg("Failed to get parameters for PV "+ name());
                logging::error("getVardef: %s", msg.c_str());
                json jmsg = createAsyncErrMsg(m_para.alias, msg);
                m_notifySocket.send(jmsg.dump());
                return 1;
            }
        } else {
            m_state = Armed;
        }
    }

    fieldName = m_fieldName;
    xtcType   = xtype[m_type];
    rank      = m_rank;
    if (m_firstDimOverride)
    {
      rank = 2;                         // Override rank
      logging::warning("%s rank overridden from %zu to %zu\n",
                       name().c_str(), m_rank, rank);
    }

    m_payloadSize = m_nelem * Name::get_element_size(xtcType);

    return 0;
}

void PvMonitor::startup()
{
    pvQueue.startup();
    bufferFreelist.startup();
    size_t bufSize = sizeof(EbDgram) + sizeof(DataDsc) + m_payloadSize;
    m_buffer.resize(pvQueue.size() * bufSize);
    for (unsigned i = 0; i < pvQueue.size(); ++i) {
        bufferFreelist.push(reinterpret_cast<Dgram*>(&m_buffer[i * bufSize]));
    }

    m_state = Ready;
}

void PvMonitor::shutdown()
{
    m_state = NotReady;

    pvQueue.shutdown();
    bufferFreelist.shutdown();

    m_nUpdates = 0;
    m_nMissed  = 0;
}

void PvMonitor::onConnect()
{
    logging::info("PV %s connected", name().c_str());

    if (m_para.verbose) {
        if (printStructure())
            logging::error("onConnect: printStructure() failed");
    }
}

void PvMonitor::onDisconnect()
{
    auto msg("PV "+ name() + " disconnected");
    logging::error("%s", msg.c_str());
    json jmsg = createAsyncErrMsg(m_para.alias, msg);
    m_notifySocket.send(jmsg.dump());
}

void PvMonitor::updated()
{
    if (m_state == Ready) {
        int64_t seconds;
        int32_t nanoseconds;
        getTimestampEpics(seconds, nanoseconds);
        TimeStamp timestamp(seconds, nanoseconds);

        // @todo: Revisit: Needed?
        //// Protect against namesLookup not being stable before Enable
        //if (m_running.load(std::memory_order_relaxed)) {
        ++m_nUpdates;
        logging::debug("%s updated @ %u.%09u", name().c_str(), timestamp.seconds(), timestamp.nanoseconds());

        Dgram* dgram;
        if (bufferFreelist.try_pop(dgram)) { // If a buffer is available...
            //static uint64_t last_ts = 0;
            //uint64_t ts = timestamp.to_ns();
            //int64_t  dT = ts - last_ts;
            //printf("  PV:  %u.%09u, dT %9ld, ts %18lu, last %18lu\n", timestamp.seconds(), timestamp.nanoseconds(), dT, ts, last_ts);
            //if (dT > 0)  last_ts = ts;

            dgram->time = timestamp;         // Save the PV's timestamp
            dgram->xtc = {{TypeId::Parent, 0}, {m_id}};

            size_t      bufSize = sizeof(DataDsc) + m_payloadSize;
            const void* bufEnd  = dgram->xtc.payload() + bufSize;
            DataDsc*    payload = (DataDsc*)dgram->xtc.alloc(bufSize, bufEnd);
            auto        size    = getData(&payload->data, m_payloadSize, payload->shape);
            if (size > m_payloadSize) {      // Check actual size vs available size
                logging::debug("Truncated: Buffer of size %zu is too small for payload of size %zu for %s\n",
                               m_payloadSize, size, name().c_str());
                dgram->xtc.damage.increase(Damage::Truncated);
            }

            if (m_firstDimOverride != 0) {
                payload->shape[1] = payload->shape[0] / m_firstDimOverride;
                payload->shape[0] = m_firstDimOverride;
            }
            pvQueue.push(dgram);
        }
        else {
            ++m_nMissed;                     // Else count it as missed
        }
        //}
    }
    else {                              // State is NotReady
        std::lock_guard<std::mutex> lock(m_mutex);

        if (m_state != Armed) {
            if (!this->Pds_Epics::PvMonitorBase::getParams(m_type, m_nelem, m_rank))  {
                m_state = Armed;
            }
        }
        m_condition.notify_one();
    }
}

void PvMonitor::timeout(const TimeStamp& timestamp)
{
    Dgram* pvDg;
    if (pvQueue.peek(pvDg)) {
        if (!(pvDg->time > timestamp)) { // Discard PV if older than the timeout timestamp
            logging::debug("PV timed out!! "
                           "TimeStamp:  %u.%09u [0x%08x%04x.%05x], age %ld ms",
                           pvDg->time.seconds(),  pvDg->time.nanoseconds(),
                           pvDg->time.seconds(), (pvDg->time.nanoseconds()>>16)&0xfffe, pvDg->time.nanoseconds()&0x1ffff,
                           _deltaT<ms_t>(pvDg->time));
            pvQueue.try_pop(pvDg);      // Actually consume the element
            bufferFreelist.push(pvDg);  // Return buffer to freelist
        }
    }
}


class Pgp : public PgpReader
{
public:
    Pgp(const Parameters& para, DrpBase& drp, Detector* det, const bool& running) :
        PgpReader(para, drp.pool, MAX_RET_CNT_C, 32),
        m_det(det), m_tebContributor(drp.tebContributor()), m_running(running),
        m_available(0), m_current(0), m_nDmaRet(0)
    {
        m_nodeId = drp.nodeId();
        if (drp.pool.setMaskBytes(para.laneMask, 0)) {
            logging::error("Failed to allocate lane/vc");
        }
    }

    EbDgram* next(uint32_t& evtIndex);
    const uint64_t nDmaRet() { return m_nDmaRet; }
private:
    EbDgram* _handle(uint32_t& evtIndex);
    Detector* m_det;
    Eb::TebContributor& m_tebContributor;
    static const int MAX_RET_CNT_C = 100;
    const bool& m_running;
    int32_t m_available;
    int32_t m_current;
    unsigned m_nodeId;
    uint64_t m_nDmaRet;
};

EbDgram* Pgp::_handle(uint32_t& pebbleIndex)
{
    const TimingHeader* timingHeader = handle(m_det, m_current);
    if (!timingHeader)  return nullptr;

    uint32_t pgpIndex = timingHeader->evtCounter & (m_pool.nDmaBuffers() - 1);
    PGPEvent* event = &m_pool.pgpEvents[pgpIndex];
    // No need to check for a broken event since we don't get indices for those

    // make new dgram in the pebble
    // It must be an EbDgram in order to be able to send it to the MEB
    pebbleIndex = event->pebbleIndex;
    Src src = m_det->nodeId;
    EbDgram* dgram = new(m_pool.pebble[pebbleIndex]) EbDgram(*timingHeader, src, m_para.rogMask);

    // Collect indices of DMA buffers that can be recycled and reset event
    freeDma(event);

    return dgram;
}

EbDgram* Pgp::next(uint32_t& evtIndex)
{
    // get new buffers
    if (m_current == m_available) {
        m_current = 0;
        m_available = read();
        m_nDmaRet = m_available;
        if (m_available == 0) {
            return nullptr;
        }
    }

    EbDgram* dgram = _handle(evtIndex);
    m_current++;
    return dgram;
}


PvDetector::PvDetector(PvParameters& para,
                       DrpBase&      drp) :
    XpmDetector      (&para, &drp.pool),
    m_para           (para),
    m_drp            (drp),
    m_evtQueue       (drp.pool.nbuffers()),
    m_terminate      (false),
    m_running        (false)
{
}

unsigned PvDetector::connect(std::string& msg)
{
    // Revisit: firstDim should be able to be different for each PV (but perhaps it's obsolete)
    uint32_t firstDimDef = 0;
    if (m_para.kwargs.find("firstdim") != m_para.kwargs.end()) {
        firstDimDef = std::stoul(m_para.kwargs["firstdim"]);
    }

    unsigned id = 0;
    for (auto& pvSpec : m_para.pvSpecs) {
        // Parse the pvSpec string of the form "[<alias>=][<provider>/]<PV name>[.<field>][,firstDim]"
        try {
            std::string alias    = m_para.detName;
            std::string pvName   = pvSpec;
            std::string provider = "pva";
            std::string field    = "value";
            uint32_t    firstDim = firstDimDef;
            auto pos = pvSpec.find("=", 0);
            if (pos != std::string::npos) { // Parse alias
                alias  = pvSpec.substr(0, pos);
                pvName = pvSpec.substr(pos+1);
            }
            pos = pvSpec.find("/", 0);
            if (pos != std::string::npos) { // Parse provider
                provider = pvSpec.substr(0, pos);
                pvName   = pvSpec.substr(pos+1);
            }
            pos = pvSpec.find(".", 0);
            if (pos != std::string::npos) { // Parse field name
                field  = pvSpec.substr(pos+1);
                pvName = pvSpec.substr(0, pos);
            }
            pos = pvSpec.find(",", 0);
            if (pos != std::string::npos) { // Parse firstDim value
                firstDim = std::stoul(pvSpec.substr(pos+1));
                field    = pvSpec.substr(0, pos);
            }
            std::string request = provider == "pva" ? "field(value,timeStamp,dimension)"
                                                    : "field(value,timeStamp)";
            if (field != "value" && field != "timeStamp" && (provider != "pva" || field != "dimension")) {
                pos = request.find(")", 0);
                if (pos  != std::string::npos) {
                    request = request.substr(0, pos) + "," + field + ")";
                }
            }

            auto pvMonitor = std::make_shared<PvMonitor>(m_para,
                                                         alias, pvName, provider, request, field,
                                                         id++, m_evtQueue.size(), firstDim);
            m_pvMonitors.push_back(pvMonitor);
        }
        catch(std::string& error) {
            logging::error("Failed to create PvMonitor for '%s': %s",
                           pvSpec.c_str(), error.c_str());
            m_pvMonitors.clear();
            msg = error;
            return 1;
        }
    }

    return 0;
}

unsigned PvDetector::disconnect()
{
    m_pvMonitors.clear();
    return 0;
}

//std::string PvDetector::sconfigure(const std::string& config_alias, Xtc& xtc, const void* bufEnd)
unsigned PvDetector::configure(const std::string& config_alias, Xtc& xtc, const void* bufEnd)
{
    logging::info("PV configure");

    if (XpmDetector::configure(config_alias, xtc, bufEnd))
        return 1;

    m_exporter = std::make_shared<MetricExporter>();
    if (m_drp.exposer()) {
        m_drp.exposer()->RegisterCollectable(m_exporter);
    }

    for (auto& pvMonitor : m_pvMonitors) {
        // Set up the names for L1Accept data
        Alg     rawAlg("raw", 1, 0, 0);
        NamesId rawNamesId(nodeId, RawNamesIndex + pvMonitor->id());
        Names&  rawNames = *new(xtc, bufEnd) Names(bufEnd,
                                                   pvMonitor->alias().c_str(), rawAlg,
                                                   m_para.detType.c_str(), m_para.serNo.c_str(), rawNamesId);
        std::string    fieldName;
        Name::DataType xtcType;
        int            rank;
        if (pvMonitor->getParams(fieldName, xtcType, rank)) {
            return 1;
        }

        RawDef rawDef(fieldName, xtcType, rank);
        rawNames.add(xtc, bufEnd, rawDef);
        m_namesLookup[rawNamesId] = NameIndex(rawNames);
    }

    // Set up the names for PvDetector informational data
    Alg     infoAlg("pvdetinfo", 1, 0, 0);
    NamesId infoNamesId(nodeId, InfoNamesIndex + m_pvMonitors.size());
    Names&  infoNames = *new(xtc, bufEnd) Names(bufEnd,
                                                ("pvdetinfo_" + m_para.detName).c_str(), infoAlg,
                                                "pvdetinfo", "detnum1234", infoNamesId);
    InfoDef infoDef(m_para.detName);
    infoNames.add(xtc, bufEnd, infoDef);
    m_namesLookup[infoNamesId] = NameIndex(infoNames);

    // add dictionary of information for each epics detname above.
    // first name is required to be "keys".  keys and values
    // are delimited by ",".
    CreateData cd(xtc, bufEnd, m_namesLookup, infoNamesId);
    std::string str = "";
    for (auto& pvMonitor : m_pvMonitors)
        str = str + pvMonitor->alias() + ",";
    cd.set_string(InfoDef::keys,    str.substr(0, str.length()-1).c_str());
    str = "";
    for (auto& pvMonitor : m_pvMonitors)
        str = str + pvMonitor->name() + "\n";
    cd.set_string(InfoDef::detName, str.substr(0, str.length()-1).c_str());

    // (Re)initialize the queues
    m_evtQueue.startup();
    for (auto& pvMonitor : m_pvMonitors) {
        pvMonitor->startup();
    }

    m_terminate.store(false, std::memory_order_release);

    m_workerThread = std::thread{&PvDetector::_worker, this};

    //    return std::string();
    return 0;
}

unsigned PvDetector::unconfigure()
{
    if (m_exporter)  m_exporter.reset();

    m_terminate.store(true, std::memory_order_release);
    if (m_workerThread.joinable()) {
        m_workerThread.join();
    }
    m_evtQueue.shutdown();
    for (auto& pvMonitor : m_pvMonitors) {
        pvMonitor->shutdown();
    }
    m_namesLookup.clear();   // erase all elements

    return 0;
}

void PvDetector::event(Dgram& dgram, const void* bufEnd, const Xtc& pvXtc)
{
    NamesId namesId(nodeId, RawNamesIndex + pvXtc.src.value());
    CreateData cd(dgram.xtc, bufEnd, m_namesLookup, namesId);
    DataDsc* payload = reinterpret_cast<DataDsc*>(pvXtc.payload());
    uint32_t* shape = payload->shape;
    void* data = &payload->data;
    size_t size = pvXtc.extent - ((char*)data - (char*)payload); // Exclude shape info
    void* ptr = cd.get_ptr(); // Fetch a pointer to the next part of contiguous memory
    cd.set_array_shape(RawDef::field, shape); // Allocate the space before filling it
    memcpy(ptr, data, size);  // size is the same as the amount of space allocated
    dgram.xtc.damage.increase(pvXtc.damage.value());
}

void PvDetector::_worker()
{
    // setup monitoring
    std::map<std::string, std::string> labels{{"instrument", m_para.instrument},
                                              {"partition", std::to_string(m_para.partition)},
                                              {"detname", m_para.detName},
                                              {"detseg", std::to_string(m_para.detSegment)},
                                              {"alias", m_para.alias}};
    m_nEvents = 0;
    m_exporter->add("drp_event_rate", labels, MetricType::Rate,
                    [&](){return m_nEvents;});
    m_nUpdates = 0;
    m_exporter->add("drp_update_rate", labels, MetricType::Rate,
                    [&](){ uint64_t nUpdates = 0;
                           for (const auto& pvMonitor : m_pvMonitors)
                               nUpdates += pvMonitor->nUpdates();
                           m_nUpdates = nUpdates;
                           return m_nUpdates; });
    m_nMatch = 0;
    m_exporter->add("drp_match_count", labels, MetricType::Counter,
                    [&](){return m_nMatch;});
    m_nEmpty = 0;
    m_exporter->add("drp_empty_count", labels, MetricType::Counter,
                    [&](){return m_nEmpty;});
    m_nMissed = 0;
    m_exporter->add("drp_miss_count", labels, MetricType::Counter,
                    [&](){ uint64_t nMissed = 0;
                           for (const auto& pvMonitor : m_pvMonitors)
                               nMissed += pvMonitor->nMissed();
                           m_nMissed = nMissed;
                           return m_nMissed; });
    m_nTooOld = 0;
    m_exporter->add("drp_tooOld_count", labels, MetricType::Counter,
                    [&](){return m_nTooOld;});
    m_nTimedOut = 0;
    m_exporter->add("drp_timeout_count", labels, MetricType::Counter,
                    [&](){return m_nTimedOut;});
    m_timeDiff = 0;
    m_exporter->add("drp_time_diff", labels, MetricType::Gauge,
                    [&](){return m_timeDiff;});

    m_exporter->add("drp_worker_input_queue", labels, MetricType::Gauge,
                    [&](){return m_evtQueue.guess_size();});
    m_exporter->constant("drp_worker_queue_depth", labels, m_evtQueue.size());

    // Borrow this for awhile
    m_exporter->add("drp_worker_output_queue", labels, MetricType::Gauge,
                    [&](){return m_pvMonitors[0]->pvQueue.guess_size();});

    Pgp pgp(m_para, m_drp, this, m_running);

    m_exporter->add("drp_num_dma_ret", labels, MetricType::Gauge,
                    [&](){return pgp.nDmaRet();});
    m_exporter->add("drp_pgp_byte_rate", labels, MetricType::Rate,
                    [&](){return pgp.dmaBytes();});
    m_exporter->add("drp_dma_size", labels, MetricType::Gauge,
                    [&](){return pgp.dmaSize();});
    m_exporter->add("drp_th_latency", labels, MetricType::Gauge,
                    [&](){return pgp.latency();});
    m_exporter->add("drp_num_dma_errors", labels, MetricType::Gauge,
                    [&](){return pgp.nDmaErrors();});
    m_exporter->add("drp_num_no_common_rog", labels, MetricType::Gauge,
                    [&](){return pgp.nNoComRoG();});
    m_exporter->add("drp_num_missing_rogs", labels, MetricType::Gauge,
                    [&](){return pgp.nMissingRoGs();});
    m_exporter->add("drp_num_th_error", labels, MetricType::Gauge,
                    [&](){return pgp.nTmgHdrError();});
    m_exporter->add("drp_num_pgp_jump", labels, MetricType::Gauge,
                    [&](){return pgp.nPgpJumps();});
    m_exporter->add("drp_num_no_tr_dgram", labels, MetricType::Gauge,
                    [&](){return pgp.nNoTrDgrams();});

    uint32_t contract = (1 << m_pvMonitors.size()) - 1;

    const uint64_t nsTmo = (m_para.kwargs.find("match_tmo_ms") != m_para.kwargs.end() ?
                            std::stoul(Detector::m_para->kwargs["match_tmo_ms"])      :
                            1500) * 1000000;

    while (true) {
        if (m_terminate.load(std::memory_order_relaxed)) {
            break;
        }

        uint32_t index;
        if (pgp.next(index)) {
            m_nEvents++;

            m_evtQueue.push({index, contract});

            _matchUp();
        }
        else {
            // If there are any PGP datagrams stacked up, try to match them
            // up with any PV updates that may have arrived
            _matchUp();

            // Generate a timestamp in the past for timing out PVs and PGP events
            TimeStamp timestamp(0, nsTmo);
            auto ns = _deltaT<ns_t>(timestamp);
            _timeout(timestamp.from_ns(ns));

            // Time out batches for the TEB
            m_drp.tebContributor().timeout();
        }
    }

    // Flush the DMA buffers
    pgp.flush();

    logging::info("Worker thread finished");
}

void PvDetector::_matchUp()
{
    while (true) {
        if (m_evtQueue.is_empty())  break;
        Event& evt = m_evtQueue.front();

        EbDgram* evtDg = reinterpret_cast<EbDgram*>(m_pool->pebble[evt.index]);
        TransitionId::Value service = evtDg->service();
        if (service == TransitionId::L1Accept) {
            unsigned remaining = evt.remaining;
            while (remaining) {
                unsigned id = __builtin_ffsl(remaining) - 1;
                remaining &= ~(1 << id);

                auto& pvMonitor = m_pvMonitors[id];

                Dgram* pvDg;
                if (!pvMonitor->pvQueue.peek(pvDg))  continue;

                m_timeDiff = evtDg->time.to_ns() - pvDg->time.to_ns();

                int result = _compare(evtDg->time, pvDg->time);

                logging::debug("PGP: %u.%09d, PV: %u.%09d, PGP - PV: %12ld ns, pid %014lx, svc %2d, compare %c, latency %ld ms",
                               evtDg->time.seconds(), evtDg->time.nanoseconds(),
                               pvDg->time.seconds(), pvDg->time.nanoseconds(),
                               m_timeDiff, evtDg->pulseId(), evtDg->service(),
                               result == 0 ? '=' : (result < 0 ? '<' : '>'), _deltaT<ms_t>(evtDg->time));

                if      (result == 0) { _tEvtEqPv(pvMonitor, *evtDg, *pvDg);  evt.remaining &= ~(1 << id); }
                else if (result  < 0) { _tEvtLtPv(pvMonitor, *evtDg, *pvDg);  evt.remaining &= ~(1 << id); }
                else                  { _tEvtGtPv(pvMonitor, *evtDg, *pvDg); }
            }
            if (evt.remaining)  break;  // Break so the timeout routine can run
        }
        else {
          // Find the transition dgram in the pool
          EbDgram* trDg = m_pool->transitionDgrams[evt.index];
          if (trDg)                     // nullptr can happen during shutdown
              _handleTransition(*evtDg, *trDg);
        }

        Event event;
        m_evtQueue.try_pop(event);      // Actually consume the element
        assert(event.index == evt.index);

        _sendToTeb(*evtDg, evt.index);
    }
}

void PvDetector::_handleTransition(EbDgram& evtDg, EbDgram& trDg)
{
    // Initialize the transition dgram's header
    trDg = evtDg;

    TransitionId::Value service = trDg.service();
    if (service != TransitionId::SlowUpdate) {
        // copy the temporary xtc created on phase 1 of the transition
        // into the real location
        Xtc& trXtc = transitionXtc();
        trDg.xtc = trXtc; // Preserve header info, but allocate to check fit
        const void* bufEnd = (char*)&trDg + m_para.maxTrSize;
        auto payload = trDg.xtc.alloc(trXtc.sizeofPayload(), bufEnd);
        memcpy(payload, (const void*)trXtc.payload(), trXtc.sizeofPayload());

        if (service == TransitionId::Enable) {
            m_running = true;
        }
        else if (service == TransitionId::Disable) {
            m_running = false;
        }
    }
}

void PvDetector::_tEvtEqPv(std::shared_ptr<PvMonitor>& pvMonitor, EbDgram& evtDg, const Dgram& pvDg)
{
    auto bufEnd = (char*)&evtDg + m_pool->pebble.bufferSize();
    event(evtDg, bufEnd, pvDg.xtc);

    ++m_nMatch;
    logging::debug("PV matches PGP!!  "
                   "TimeStamps: PV %u.%09u == PGP %u.%09u",
                   pvDg.time.seconds(), pvDg.time.nanoseconds(),
                   evtDg.time.seconds(), evtDg.time.nanoseconds());

    Dgram* dgram;
    pvMonitor->pvQueue.try_pop(dgram);     // Actually consume the element
    pvMonitor->bufferFreelist.push(dgram); // Return buffer to freelist
}

void PvDetector::_tEvtLtPv(std::shared_ptr<PvMonitor>& pvMonitor, EbDgram& evtDg, const Dgram& pvDg)
{
    // Because PVs show up in time order, when the most recent PV is younger
    // than the PGP event (t(PV) > t(PGP)), we know that no older PV will show
    // up to possibly match the PGP timestamp.  Thus, mark the event damaged and
    // leave the PV on the queue to perhaps be matched with a newer PGP event.
    evtDg.xtc.damage.increase(Damage::MissingData);

    ++m_nEmpty;
    logging::debug("PV too young!!    "
                   "TimeStamps: PGP %u.%09u < PV %u.%09u",
                   evtDg.time.seconds(), evtDg.time.nanoseconds(),
                   pvDg.time.seconds(), pvDg.time.nanoseconds());
}

void PvDetector::_tEvtGtPv(std::shared_ptr<PvMonitor>& pvMonitor, EbDgram& evtDg, const Dgram& pvDg)
{
    // Because PGP events show up in time order, when the most recent PV is older
    // than the PGP event (t(PV) < t(PGP)), we know that no older PGP event will
    // show up to match the PV's timestamp.  Thus, the PV is discarded and
    // the PGP event is left on the queue to perhaps be matched with a newer PV.
    ++m_nTooOld;
    logging::debug("PV too old!!      "
                   "TimeStamps: PGP %u.%09u > PV %u.%09u [0x%08x%04x.%05x > 0x%08x%04x.%05x]",
                   evtDg.time.seconds(), evtDg.time.nanoseconds(),
                   pvDg.time.seconds(), pvDg.time.nanoseconds(),
                   evtDg.time.seconds(), (evtDg.time.nanoseconds()>>16)&0xfffe, evtDg.time.nanoseconds()&0x1ffff,
                   pvDg.time.seconds(), (pvDg.time.nanoseconds()>>16)&0xfffe, pvDg.time.nanoseconds()&0x1ffff);

    Dgram* dgram;
    pvMonitor->pvQueue.try_pop(dgram);     // Actually consume the element
    pvMonitor->bufferFreelist.push(dgram); // Return buffer to freelist
}

void PvDetector::_timeout(const TimeStamp& timeout)
{
    // Time out older PV updates
    for (auto& pvMonitor : m_pvMonitors) {
        pvMonitor->timeout(timeout);
    }

    // Time out older pending PGP datagrams
    Event event;
    if (!m_evtQueue.peek(event))  return;

    EbDgram& dgram = *reinterpret_cast<EbDgram*>(m_pool->pebble[event.index]);
    if (dgram.time > timeout)  return;  // dgram is newer than the timeout timestamp

    Event evt;
    m_evtQueue.try_pop(evt);            // Actually consume the element
    assert(evt.index == event.index);

    if (dgram.service() == TransitionId::L1Accept) {
        // No PV data so mark event as damaged
        dgram.xtc.damage.increase(Damage::TimedOut);
        ++m_nTimedOut;
        logging::debug("Event timed out!! "
                       "TimeStamp:  %u.%09u [0x%08x%04x.%05x], age %ld ms",
                       dgram.time.seconds(), dgram.time.nanoseconds(),
                       dgram.time.seconds(), (dgram.time.nanoseconds()>>16)&0xfffe, dgram.time.nanoseconds()&0x1ffff,
                       _deltaT<ms_t>(dgram.time));
    }

    _sendToTeb(dgram, event.index);
}

void PvDetector::_sendToTeb(const EbDgram& dgram, uint32_t index)
{
    // Make sure the datagram didn't get too big
    const size_t size = sizeof(dgram) + dgram.xtc.sizeofPayload();
    const size_t maxSize = (dgram.service() == TransitionId::L1Accept)
                         ? m_pool->pebble.bufferSize()
                         : m_para.maxTrSize;
    if (size > maxSize) {
        logging::critical("%s Dgram of size %zd overflowed buffer of size %zd", TransitionId::name(dgram.service()), size, maxSize);
        throw "Dgram overflowed buffer";
    }

    auto l3InpBuf = m_drp.tebContributor().fetch(index);
    EbDgram* l3InpDg = new(l3InpBuf) EbDgram(dgram);
    if (l3InpDg->isEvent()) {
        auto triggerPrimitive = m_drp.triggerPrimitive();
        if (triggerPrimitive) { // else this DRP doesn't provide input
            const void* bufEnd = (char*)l3InpDg + sizeof(*l3InpDg) + triggerPrimitive->size();
            triggerPrimitive->event(*m_pool, index, dgram.xtc, l3InpDg->xtc, bufEnd); // Produce
        }
    }
    m_drp.tebContributor().process(l3InpDg);
}


PvApp::PvApp(PvParameters& para) :
    CollectionApp(para.collectionHost, para.partition, "drp", para.alias),
    m_drp(para, context()),
    m_para(para),
    m_pvDetector(std::make_unique<PvDetector>(para, m_drp)),
    m_det(m_pvDetector.get()),
    m_unconfigure(false)
{
    Py_Initialize();                    // for use by configuration

    if (m_det == nullptr) {
        logging::critical("Error !! Could not create Detector object for %s", m_para.detType.c_str());
        throw "Could not create Detector object for " + m_para.detType;
    }
    logging::info("Ready for transitions");
}

PvApp::~PvApp()
{
    // Try to take things down gracefully when an exception takes us off the
    // normal path so that the most chance is given for prints to show up
    handleReset(json({}));

    Py_Finalize();                      // for use by configuration
}

void PvApp::_disconnect()
{
    m_drp.disconnect();
    m_det->shutdown();
    m_pvDetector->disconnect();
}

void PvApp::_unconfigure()
{
    m_drp.pool.shutdown();  // Release Tr buffer pool
    m_drp.unconfigure();    // TebContributor must be shut down before the worker
    m_pvDetector->unconfigure();
    m_unconfigure = false;
}

json PvApp::connectionInfo(const nlohmann::json& msg)
{
    std::string ip = m_para.kwargs.find("ep_domain") != m_para.kwargs.end()
                   ? getNicIp(m_para.kwargs["ep_domain"])
                   : getNicIp(m_para.kwargs["forceEnet"] == "yes");
    logging::debug("nic ip  %s", ip.c_str());
    json body = {{"connect_info", {{"nic_ip", ip}}}};
    json info = m_det->connectionInfo(msg);
    body["connect_info"].update(info);
    json bufInfo = m_drp.connectionInfo(ip);
    body["connect_info"].update(bufInfo);
    return body;
}

void PvApp::connectionShutdown()
{
    m_drp.shutdown();
}

void PvApp::_error(const std::string& which, const nlohmann::json& msg, const std::string& errorMsg)
{
    json body = json({});
    body["err_info"] = errorMsg;
    json answer = createMsg(which, msg["header"]["msg_id"], getId(), body);
    reply(answer);
}

void PvApp::handleConnect(const nlohmann::json& msg)
{
    std::string errorMsg = m_drp.connect(msg, getId());
    if (!errorMsg.empty()) {
        logging::error("Error in DrpBase::connect");
        logging::error("%s", errorMsg.c_str());
        _error("connect", msg, errorMsg);
        return;
    }

    m_det->nodeId = m_drp.nodeId();
    m_det->connect(msg, std::to_string(getId()));

    unsigned rc = m_pvDetector->connect(errorMsg);
    if (!errorMsg.empty()) {
        if (!rc) {
            logging::warning(("PvDetector::connect: " + errorMsg).c_str());
            json warning = createAsyncWarnMsg(m_para.alias, errorMsg);
            reply(warning);
        }
        else {
            logging::error(("PvDetector::connect: " + errorMsg).c_str());
            _error("connect", msg, errorMsg);
            return;
        }
    }

    json body = json({});
    json answer = createMsg("connect", msg["header"]["msg_id"], getId(), body);
    reply(answer);
}

void PvApp::handleDisconnect(const json& msg)
{
    // Carry out the queued Unconfigure, if there was one
    if (m_unconfigure) {
        _unconfigure();
    }

    _disconnect();

    json body = json({});
    reply(createMsg("disconnect", msg["header"]["msg_id"], getId(), body));
}

void PvApp::handlePhase1(const json& msg)
{
    std::string key = msg["header"]["key"];
    logging::debug("handlePhase1 for %s in PvDetectorApp", key.c_str());

    Xtc& xtc = m_det->transitionXtc();
    xtc = {{TypeId::Parent, 0}, {m_det->nodeId}};
    auto bufEnd = m_det->trXtcBufEnd();

    json phase1Info{ "" };
    if (msg.find("body") != msg.end()) {
        if (msg["body"].find("phase1Info") != msg["body"].end()) {
            phase1Info = msg["body"]["phase1Info"];
        }
    }

    json body = json({});

    if (key == "configure") {
        if (m_unconfigure) {
            _unconfigure();
        }

        std::string errorMsg = m_drp.configure(msg);
        if (!errorMsg.empty()) {
            errorMsg = "Phase 1 error: " + errorMsg;
            logging::error("%s", errorMsg.c_str());
            _error(key, msg, errorMsg);
            return;
        }

        std::string config_alias = msg["body"]["config_alias"];
        unsigned error = m_det->configure(config_alias, xtc, bufEnd);
        if (error) {
            std::string errorMsg = "Failed transition phase 1";
            logging::error("%s", errorMsg.c_str());
            _error(key, msg, errorMsg);
            return;
        }

        m_drp.runInfoSupport(xtc, bufEnd, m_det->namesLookup());
        m_drp.chunkInfoSupport(xtc, bufEnd, m_det->namesLookup());
    }
    else if (key == "unconfigure") {
        // "Queue" unconfiguration until after phase 2 has completed
        m_unconfigure = true;
    }
    else if (key == "beginrun") {
        RunInfo runInfo;
        std::string errorMsg = m_drp.beginrun(phase1Info, runInfo);
        if (!errorMsg.empty()) {
            body["err_info"] = errorMsg;
            logging::error("%s", errorMsg.c_str());
        }
        else {
            m_drp.runInfoData(xtc, bufEnd, m_det->namesLookup(), runInfo);
        }
    }
    else if (key == "endrun") {
        std::string errorMsg = m_drp.endrun(phase1Info);
        if (!errorMsg.empty()) {
            body["err_info"] = errorMsg;
            logging::error("%s", errorMsg.c_str());
        }
    }
    else if (key == "enable") {
        bool chunkRequest;
        ChunkInfo chunkInfo;
        std::string errorMsg = m_drp.enable(phase1Info, chunkRequest, chunkInfo);
        if (!errorMsg.empty()) {
            body["err_info"] = errorMsg;
            logging::error("%s", errorMsg.c_str());
        } else if (chunkRequest) {
            logging::debug("handlePhase1 enable found chunkRequest");
            m_drp.chunkInfoData(xtc, bufEnd, m_det->namesLookup(), chunkInfo);
        }
        logging::debug("handlePhase1 enable complete");
    }

    json answer = createMsg(key, msg["header"]["msg_id"], getId(), body);
    reply(answer);
}

void PvApp::handleReset(const nlohmann::json& msg)
{
    unsubscribePartition();    // ZMQ_UNSUBSCRIBE
    _unconfigure();
    _disconnect();
    connectionShutdown();
}

} // namespace Drp


int main(int argc, char* argv[])
{
    Drp::PvParameters para;
    std::string kwargs_str;
    int c;
    while((c = getopt(argc, argv, "p:o:l:D:S:C:d:u:k:P:T::M:01v")) != EOF) {
        switch(c) {
            case 'p':
                para.partition = std::stoi(optarg);
                break;
            case 'o':
                para.outputDir = optarg;
                break;
            case 'l':
                para.laneMask = std::stoul(optarg, nullptr, 16);
                break;
            case 'D':
                para.detType = optarg;  // Defaults to 'pv'
                break;
            case 'S':
                para.serNo = optarg;
                break;
            case 'u':
                para.alias = optarg;
                break;
            case 'C':
                para.collectionHost = optarg;
                break;
            case 'd':
                para.device = optarg;
                break;
            case 'k':
                kwargs_str = kwargs_str.empty()
                           ? optarg
                           : kwargs_str + "," + optarg;
                break;
            case 'P':
                para.instrument = optarg;
                break;
            case 'M':
                para.prometheusDir = optarg;
                break;
            //  Indicate level of timestamp matching (ugh)
            case '0':
                tsMatchDegree = 0;
                break;
            case '1':
                fprintf(stderr, "Option -1 is disabled\n");  exit(EXIT_FAILURE);
                tsMatchDegree = 1;
                break;
            case 'v':
                ++para.verbose;
                break;
            default:
                return 1;
        }
    }

    switch (para.verbose) {
        case 0:  logging::init(para.instrument.c_str(), LOG_INFO);   break;
        default: logging::init(para.instrument.c_str(), LOG_DEBUG);  break;
    }
    logging::info("logging configured");
    if (para.instrument.empty()) {
        logging::warning("-P: instrument name is missing");
    }
    // Check required parameters
    if (para.partition == unsigned(-1)) {
        logging::critical("-p: partition is mandatory");
        return 1;
    }
    if (para.device.empty()) {
        logging::critical("-d: device is mandatory");
        return 1;
    }
    if (para.alias.empty()) {
        logging::critical("-u: alias is mandatory");
        return 1;
    }

    // Only one lane is supported by this DRP
    if (std::bitset<PGP_MAX_LANES>(para.laneMask).count() != 1) {
        logging::critical("-l: lane mask must have only 1 bit set");
        return 1;
    }

    // Allow detType to be overridden, but generally, psana will expect 'pv'
    if (para.detType.empty()) {
      para.detType = "pv";
    }

    // Alias must be of form <detName>_<detSegment>
    size_t found = para.alias.rfind('_');
    if ((found == std::string::npos) || !isdigit(para.alias.back())) {
        logging::critical("-u: alias must have _N suffix");
        return 1;
    }
    para.detName = para.alias.substr(0, found);
    para.detSegment = std::stoi(para.alias.substr(found+1, para.alias.size()));

    // Provider is "pva" (default) or "ca"
    if (optind >= argc) {
        logging::critical("At least one PV ([<alias>=][<provider>/]<PV name>[.<field>][,<firstDim>]) is required");
        return 1;
    }
    unsigned i = 0;
    while (optind < argc) {
        para.pvSpecs.push_back({argv[optind++]}); // [<alias>=][<provider>/]<PV name>[.<field>][,<firstDim>]

        if (i++ > 31) {
            logging::critical("Too many PVs provided; max is %u", i);
            return 1;
        }
    }

    para.maxTrSize = 256 * 1024;
    try {
        get_kwargs(kwargs_str, para.kwargs);
        for (const auto& kwargs : para.kwargs) {
            if (kwargs.first == "forceEnet")      continue;
            if (kwargs.first == "ep_fabric")      continue;
            if (kwargs.first == "ep_domain")      continue;
            if (kwargs.first == "ep_provider")    continue;
            if (kwargs.first == "sim_length")     continue;  // XpmDetector
            if (kwargs.first == "timebase")       continue;  // XpmDetector
            if (kwargs.first == "pebbleBufSize")  continue;  // DrpBase
            if (kwargs.first == "pebbleBufCount") continue;  // DrpBase
            if (kwargs.first == "batching")       continue;  // DrpBase
            if (kwargs.first == "directIO")       continue;  // DrpBase
            if (kwargs.first == "pva_addr")       continue;  // DrpBase
            if (kwargs.first == "firstdim")       continue;
            if (kwargs.first == "match_tmo_ms")   continue;
            logging::critical("Unrecognized kwarg '%s=%s'\n",
                              kwargs.first.c_str(), kwargs.second.c_str());
            return 1;
        }

        Drp::PvApp(para).run();
        return 0;
    }
    catch (std::exception& e)  { logging::critical("%s", e.what()); }
    catch (std::string& e)     { logging::critical("%s", e.c_str()); }
    catch (char const* e)      { logging::critical("%s", e); }
    catch (...)                { logging::critical("Default exception"); }
    return EXIT_FAILURE;
}
