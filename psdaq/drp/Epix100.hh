#pragma once

#include "BEBDetector.hh"
#include "psdaq/service/Semaphore.hh"

namespace Drp {

class Epix100 : public BEBDetector
{
public:
    Epix100(Parameters* para, MemPool* pool);
    ~Epix100();
    nlohmann::json connectionInfo() override;
    unsigned enable   (XtcData::Xtc& xtc, const nlohmann::json& info) override;
    unsigned disable  (XtcData::Xtc& xtc, const nlohmann::json& info) override;
    void slowupdate(XtcData::Xtc&) override;
    bool scanEnabled() override;
    void shutdown() override;
    void write_image(XtcData::Xtc&, std::vector< XtcData::Array<uint8_t> >&, XtcData::NamesId&);
protected:
    void           _connect  (PyObject*) override;
    unsigned       _configure(XtcData::Xtc&, XtcData::ConfigIter&) override;
    void           _event    (XtcData::Xtc&,
                              std::vector< XtcData::Array<uint8_t> >&) override;
public:
    void           monStreamEnable ();
    void           monStreamDisable();
protected:
    Pds::Semaphore    m_env_sem;
    bool              m_env_empty;
    XtcData::NamesId  m_evtNamesId[2];
  };

}