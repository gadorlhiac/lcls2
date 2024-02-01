from psdaq.configdb.get_config import get_config

from psdaq.configdb.scan_utils import *
from psdaq.configdb.xpmmini import *
from psdaq.cas.xpm_utils import timTxId
import rogue
import high_rate_encoder_dev
import time
import json
import IPython
from collections import deque
import logging
import weakref

import pyrogue as pr
import surf.protocols.clink as clink
import rogue.interfaces.stream

hr_enc = None
pv = None
lm: int = 1

#FEB parameters
lane: int = 0
chan: int = 0
ocfg = None
group = None

def cycle_timing_link(hr_enc):
    """Cycles timing link if it is stuck.

    This function should not need to be called with recent firmware
    updates. Previously empirically found by cpo that cycling LCLS1
    and XpmMini timing could be used to get the timing feedback link
    to lock.
    """
    nbad = 0
    while 1:
        # check to see if timing is stuck
        sof1 = hr_enc.App.TimingRx.TimingFrameRx.sofCount.get()
        time.sleep(0.1)
        sof2 = hr_enc.App.TimingRx.TimingFrameRx.sofCount.get()
        if sof1!=sof2: break
        nbad+=1
        print('*** Timing link stuck:',sof1,sof2,'resetting. Iteration:', nbad)
        #  Empirically found that we need to cycle to LCLS1 timing
        #  to get the timing feedback link to lock
        #  cpo: switch this to XpmMini which recovers from more issues?
        hr_enc.App.TimingRx.ConfigureXpmMini()
        time.sleep(3.5)
        print("Before LCLS timing")
        hr_enc.App.TimingRx.ConfigLclsTimingV2()
        print("After LCLS timing")
        time.sleep(3.5)


def hrencoder_init(arg,dev='/dev/datadev_0',lanemask=1,xpmpv=None,timebase="186M",verbosity=0):

    global pv
    global hr_enc
    global lm
    global lane

    print('hrencoder_init')

    lm = lanemask
    lane = (lm&-lm).bit_length()-1
    assert(lm==(1<<lane)) # check that lanemask only has 1 bit for piranha4
    myargs = {
        'dev': dev,
        'lane': lane,
        #'dataVcEn': False,                       # Whether to open data path in devGui
        'defaultFile': "/cds/home/d/dorlhiac/Repos/high-rate-encoder-dev/software/config/",#"config/defaults.yml",   # Config file for defaults
        'standAloneMode': False,                    # False = use fiber timing, True = local timing
        'pollEn': False,                           # Enable automatic register polling (True by default)
        'initRead': False,                         # Read all registers at init (True by default)
        # 'promProg': False,                       # Disable all devs not for PROM programming
        'zmqSrvEn': True,                        # Include ZMQ server (True by default) - for devGui + DAQ together
    }

    # in older versions we didn't have to use the "with" statement
    # but now the register accesses don't seem to work without it -cpo
    hr_enc = high_rate_encoder_dev.Root(**myargs)
    #hr_enc = None
    # Get ClinkDevRoot.start() and stop() called
    weakref.finalize(hr_enc, hr_enc.stop)
    hr_enc.start()

    ##cycle_timing_link(hr_enc)
    hr_enc.App.TimingRx.ConfigLclsTimingV2()
    time.sleep(0.1)

    return hr_enc

def hrencoder_init_feb(slane=None,schan=None):
    # cpo: ignore "slane" because lanemask is given to piranha4_init() above
    global chan
    if schan is not None:
        chan = int(schan)

#def hrencoder_connect(hr_enc):
def hrencoder_connectionInfo(hr_enc, alloc_json_str):
    global lane
    global chan

    print('hrencoder_connect')

    txId = timTxId('hrencoder')

    rxId = hr_enc.App.TimingRx.TriggerEventManager.XpmMessageAligner.RxId.get()
    hr_enc.App.TimingRx.TriggerEventManager.XpmMessageAligner.TxId.set(txId)

    hr_enc.StopRun()

    d = {}
    d['paddr'] = rxId # 0xffb56804 for teststand testing

    return d

def user_to_expert(hr_enc, cfg, full=False):
    global group

    d = {}
    print("hrencoder_user_to_expert")
    if "user" in cfg and "start_ns" in cfg["user"]:
        partitionDelay: int = getattr(
            hr_enc.App.TimingRx.TriggerEventManager.XpmMessageAligner,
            f"PartitionDelay[{group}]"
        ).get()
        rawStart: int = cfg["user"]["start_ns"]
        triggerDelay: int = int(rawStart*1300/7000 - partitionDelay*200)

        print(f"group {group}  partitionDelay {partitionDelay}  rawStart {rawStart}  triggerDelay {triggerDelay}")

        if triggerDelay < 0:
            # Math here doesn't match above?
            print(f"Raise start_ns >= {partitionDelay*200*7000/1300}")
            raise ValueError('triggerDelay computes to < 0')

        d['expert.App.TimingRx.TriggerEventManager.TriggerEventBuffer.TriggerDelay']=triggerDelay

    # Need to figure out if this needs to be uncommented
    # Currently leads to error
    #if full:
    #    d['expert.App.TimingRx.TriggerEventManager.TriggerEventBuffer[0].Partition']=group

    update_config_entry(cfg,ocfg,d)

def config_expert(hr_enc, cfg):
    global lane
    global chan

    # translate legal Python names to Rogue names
    rogue_translate = {#'ClinkFeb'          :'ClinkFeb[%d]'%lane,
                       #'ClinkCh'           :'Ch[%d]'%chan,
                       'TriggerEventBuffer':'TriggerEventBuffer[0]',
                       #'TrigCtrl'          :'TrigCtrl[%d]'%chan,
                       #'PllConfig0'        :'PllConfig[0]',
                       #'PllConfig1'        :'PllConfig[1]',
                       #'PllConfig2'        :'PllConfig[2]',
    }

    depth = 0
    path  = 'hr_enc'
    my_queue  =  deque([[path,depth,hr_enc,cfg]]) #contains path, dfs depth, rogue hiearchy, and daq configdb dict tree node
    while(my_queue):
        path,depth,rogue_node, configdb_node = my_queue.pop()
        #  Replace configdb lane and febch for the physical values
        if(dict is type(configdb_node)):
            for i in configdb_node:
                if i in rogue_translate:
                    my_queue.appendleft([path+"."+i,depth+1,rogue_node.nodes[rogue_translate[i]],configdb_node[i]])
                else:
                    try:
                        my_queue.appendleft([path+"."+i,depth+1,rogue_node.nodes[i],configdb_node[i]])
                    except KeyError:
                        print('Lookup failed for node [{:}] in path [{:}]'.format(i,path))

        #  Apply
        if('get' in dir(rogue_node) and 'set' in dir(rogue_node) and path != 'cl' ):
            rogue_node.set(configdb_node)


#  Apply the full configuration
def hrencoder_config(hr_enc,connect_str,cfgtype,detname,detsegm,grp):
    global ocfg
    global group
    global lane
    global chan

    print('hrencoder_config')

    group = grp

    cfg = {}
    cfg[':types:'] = {
        'alg:RO': {
            'alg:RO' : 'CHARSTR',
            'doc:RO': 'CHARSTR',
            'version:RO': ['INT32', 3],
        },
        'detName:RO': 'CHARSTR',
        'detType:RO': 'CHARSTR',
        'doc:RO': 'CHARSTR',
        'detId:RO': 'CHARSTR',
        'firmwareVersion': 'UINT32',
    }

    cfg['detName:RO'] = "rixs_hrencoder1_0"
    cfg['detType:RO'] = "hrencoder"
    cfg['doc:RO'] = "Test configuration."
    cfg['detId:RO'] = "idnum123"
    cfg['alg:RO'] = {
        'alg:RO': 'config',
        'doc:RO': 'Test',
        'version:RO': [1, 0, 0],
    }

    if hr_enc.Core.Pgp4AxiL.RxStatus.RemRxLinkReady.get() != 1:
        raise ValueError("PGP Link Down")

    trig_event_buf = getattr(
        hr_enc.App.TimingRx.TriggerEventManager, "TriggerEventBuffer[0]"
    )

    trig_event_buf.TriggerSource.set(0) # Set trigger source to XPM NOT Evr
    # This may not be the same as XpmPauseThresh for the cameralink
    # Range for this value is [0, 31] so the code as written wouldn't work
    if hasattr(trig_event_buf, "PauseThreshold"):
        trig_event_buf.PauseThreshold.set(8) # Need to set to 8

    hr_enc.App.EventBuilder.Blowoff.set(True)
    #user_to_expert(hr_enc,cfg,full=True)

    trig_event_buf.Partition.set(0x0)
    #trig_event_buf.PauseToTrig.set(0x0)
    #trig_event_buf.NotPauseToTrig.set(0xfff)

    hr_enc.App.EventBuilder.Blowoff.set(False)

    # Bypass BEB 3 (full timing stream) - not needed for encoder
    # and has minimal (no) buffer.
    hr_enc.App.EventBuilder.Bypass.set(0x4)
    hr_enc.App.EventBuilder.Timeout.set(0x0)


    #  Capture the firmware version to persist in the xtc
    cfg['firmwareVersion'] = hr_enc.Core.AxiVersion.FpgaVersion.get()
    # There is no "BuildStamp" at least as far as I can find
    #cfg['firmwareBuild'  ] = cl.ClinkPcie.AxiPcieCore.AxiVersion.BuildStamp.get()

    hr_enc.StartRun()

    trig_event_buf.enable.set(True)
    trig_event_buf.MasterEnable.set(True)

    #hr_enc.__exit__(None, None, None)
    #del hr_enc
    return json.dumps(cfg)

def hrencoder_scan_keys(update):
    global ocfg
    global hr_enc
    print("hrencoder_scan_keys")
    cfg = {}
    copy_reconfig_keys(cfg, ocfg, json.loads(update))

    user_to_expert(cl, cfg, full=False)

    for key in ("detType:RO", "detName:RO", "detId:RO", "doc:RO", "alg:RO"):
        copy_config_entry(cfg, ocfg, key)
        copy_config_entry(cfg[":types:"], ocfg[":types"], key)
    return json.dumps(cfg)

def hrencoder_update(update):
    global ocfg
    global cl
    #  extract updates
    cfg = {}
    update_config_entry(cfg,ocfg, json.loads(update))
    #  Apply group
    user_to_expert(cl,cfg,full=False)
    #  Apply config
    config_expert(cl, cfg['expert'])
    #  Retain mandatory fields for XTC translation
    for key in ('detType:RO','detName:RO','detId:RO','doc:RO','alg:RO'):
        copy_config_entry(cfg,ocfg,key)
        copy_config_entry(cfg[':types:'],ocfg[':types:'],key)
    return json.dumps(cfg)

def hrencoder_unconfig(hr_enc):
    print('hrencoder_unconfig')

    hr_enc.StopRun()

    return hr_enc
