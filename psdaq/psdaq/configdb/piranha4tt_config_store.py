from psdaq.configdb.typed_json import cdict
import psdaq.configdb.configdb as cdb
import psdaq.configdb.piranha4_config_store as piranha4
import sys
import IPython
import argparse

def piranha4tt_cdict():

    top = piranha4.piranha4_cdict()

    #  append to the help string
    help_str = top.get("help:RO")
    help_str += "\nfex.eventcodes.beam  : beam present  = AND(.incl) and not OR(.excl)"
    help_str += "\nfex.eventcodes.laser : laser present = AND(.incl) and not OR(.excl)"
    help_str += "\nfex.roi              : inclusive columns (x) and rows (y)"
    help_str += "\nfex.pedestal_adj     : extra offset to image values"
    help_str += "\nfex.signal.minvalue  : minimum signal (ADU) to report valid value"
    help_str += "\nfex..convergence     : rolling average timescale (1/N); 0 to disable correction"
    help_str += "\nfex.prescale..       : record 1/N events; 0 to disable recording"
    help_str += "\nfex.fir_weights      : edge-finding FIR constants"
    help_str += "\nfex.calib_poly       : poly coefficients for edge to time(ps)"
    top.set("help:RO", help_str, 'CHARSTR')

    #  append new fields
    top.set("fex.enable",    1, 'UINT8')

    #  assume mode is nobeam on separate events (vs nobeam in separate roi)
    top.set("fex.eventcodes.beam.incl" , [136], 'UINT8') # beam present = AND beam.incl NOR beam.excl
    top.set("fex.eventcodes.beam.excl" , [161], 'UINT8')
    top.set("fex.eventcodes.laser.incl", [67], 'UINT8') # laser present = AND laser.incl NOR laser.excl
    top.set("fex.eventcodes.laser.excl", [68], 'UINT8')

    top.set("fex.sig.roi.x0",  700, 'UINT32')
    top.set("fex.sig.roi.x1",  900, 'UINT32')
    top.set("fex.ref.enable",    0, 'UINT8')
    top.set("fex.ref.roi.x0",    0, 'UINT32')
    top.set("fex.ref.roi.x1",  298, 'UINT32')
    top.set("fex.sb.enable" ,    0, 'UINT8')
    top.set("fex.sb.roi.x0",     0, 'UINT32')
    top.set("fex.sb.roi.x1",   298, 'UINT32')

    top.define_enum('boolEnum', {'False':0, 'True':1})

    top.set("fex.pedestal_adj", 32, 'INT32')

#    top.define_enum('boolEnum', {'False':0, 'True':1})
#    top.set("fex.subtractAndNormalize" 1, 'boolEnum')

    top.set("fex.project.minvalue"   ,  0, 'UINT32')
    top.set("fex.sig.convergence" ,  1.00, 'DOUBLE') # IIR with timescale = 1/N, 0 to disable
    top.set("fex.ref.convergence" ,  1.00, 'DOUBLE') # IIR with timescale = 1/N, 0 to disable
    top.set("fex.sb.convergence"  ,  0.05, 'DOUBLE') # IIR with timescale = 1/N, 0 to disable

    top.set("fex.prescale.image"      , 1, 'UINT32') # 0=disable
    top.set("fex.prescale.projections", 1, 'UINT32') # 0=disable

    top.define_enum('recordEnum', {'None':0, 'Projection':1, 'Image':2})
    top.set("fex.ref.record"    ,  0, 'recordEnum')

    weights = [-0.0590, -0.0570, -0.0557, -0.0591, -0.0572, -0.0556, -0.0554, -0.0546, -0.0532, -0.0511, -0.0511, -0.0505, -0.0511, -0.0519, -0.0508, -0.0494, -0.0492, -0.0503, -0.0504, -0.0484, -0.0478, -0.0449, -0.0411, -0.0379, -0.0344, -0.0323, -0.0282, -0.0259, -0.0224, -0.0219, -0.0192, -0.0179, -0.0168, -0.0164, -0.0164, -0.0136, -0.0115, -0.0082, -0.0041, 0.0013, 0.0042, 0.0105, 0.0117, 0.0131, 0.0153, 0.0155, 0.0158, 0.0166, 0.0236, 0.0279, 0.0271, 0.0305, 0.0332, 0.0374, 0.0353, 0.0380, 0.0405, 0.0414, 0.0438, 0.0425, 0.0446, 0.0438, 0.0442, 0.0456, 0.0461, 0.0474, 0.0479, 0.0487, 0.0504, 0.0487, 0.0506, 0.0514, 0.0509, 0.0502, 0.0507, 0.0527, 0.0521, 0.0541, 0.0574, 0.0591]

    top.set("fex.fir_weights", weights, 'DOUBLE')

    top.set("fex.invert_weights", 0, 'boolEnum')

    calib = [1.42857, -0.00285714, 0.0]

    top.set("fex.calib_poly" , calib, 'DOUBLE') # time(ps) = sum( calib_poly[i]*edge^i )

    return top

if __name__ == "__main__":
    args = cdb.createArgs().args

    create = True
    dbname = 'configDB'     #this is the name of the database running on the server.  Only client care about this name.

    db   = 'configdb' if args.prod else 'devconfigdb'
    url  = f'https://pswww.slac.stanford.edu/ws-auth/{db}/ws/'

    mycdb = cdb.configdb(url, args.inst, create,
                         root=dbname, user=args.user, password=args.password)
    mycdb.add_alias(args.alias)
    mycdb.add_device_config('piranha4')

    top = piranha4tt_cdict()
    top.setInfo('piranha4', args.name, args.segm, args.id, 'No comment')

    if mycdb.modify_device(args.alias, top) == 0:
        print('Failed to store configuration')
        sys.exit(1)
