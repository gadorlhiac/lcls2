#!/usr/bin/env python

import sys
from time import time
from psana.detector.Utils import info_parser_arguments
from psana.detector.UtilsEpix10kaCalib import pedestals_calibration, DIR_REPO_EPIX10KA
from psana.detector.UtilsLogging import logging, DICT_NAME_TO_LEVEL, init_stream_handler
logger = logging.getLogger(__name__)

import logging
logger = logging.getLogger(__name__)
DICT_NAME_TO_LEVEL = logging._nameToLevel

SCRNAME = sys.argv[0].rsplit('/')[-1]

USAGE = 'Usage:'\
      + '\n  %s -e <experiment> -d <detector> -r <run-number(s)>' % SCRNAME\
      + '\n     [-x <xtc-directory>] [-o <output-result-directory>] [-L <logging-mode>] [...]'\
      + '\nExamples:'\
      + '\n  %s -e ueddaq02 -d epixquad -r27' % SCRNAME\
      + '\n  mpirun -n 5 %s -e ueddaq02 -d epixquad -r27 -o ./work -L DEBUG' % SCRNAME\
      + '\n  %s -e ueddaq02 -d epixquad -r83 -o ./work' % SCRNAME\
      + '\n  %s -e ueddaq02 -d epixquad -r27 -i15 -o ./work -L DEBUG' % SCRNAME\
      + '\n  %s -e rixx45619 -d epixhr -r121' % SCRNAME\
      + '\n\n  Try: %s -h' % SCRNAME


def do_main():

    t0_sec = time()

    parser = argument_parser()
    args = parser.parse_args()
    kwa = vars(args)
    #defs = vars(parser.parse_args([])) # dict of defaults only

    if len(sys.argv)<3: exit('\n%s\n' % USAGE)

    assert args.exp is not None, 'WARNING: option "-e <experiment>" MUST be specified.'
    assert args.det is not None, 'WARNING: option "-d <detector-name>" MUST be specified.'
    assert args.runs is not None, 'WARNING: option "-r <run-number(s)>" MUST be specified.'

    init_stream_handler(loglevel=args.logmode)

    logger.debug('%s\nIn epix10ka_pedestals_calibration' % (50*'_'))
    logger.debug('Command line:%s' % ' '.join(sys.argv))
    logger.info(info_parser_arguments(parser))

    pedestals_calibration(**kwa)

    logger.info('DONE, consumed time %.3f sec' % (time() - t0_sec))


def argument_parser():
    from argparse import ArgumentParser

    d_fname   = None # '/cds/data/psdm/ued/ueddaq02/xtc/ueddaq02-r0027-s000-c000.xtc2'
    d_exp     = None # 'ueddaq02'
    d_det     = None # 'epixquad'
    d_runs    = None # 1021 or 1021,1022-1025
    d_nrecs   = 800  # number of records to collect and process
    d_nrecs1  = 200  # number of records to process at 1st stage
    d_idx     = None # 0-15 for epix10ka2m, 0-3 for epix10kaquad
    d_dirxtc  = None # '/cds/data/psdm/ued/ueddaq02/xtc'
    d_dirrepo = DIR_REPO_EPIX10KA
    d_usesmd  = True
    d_logmode = 'INFO'
    d_errskip = True
    d_stepnum = None
    d_stepmax = 5
    d_evskip  = 100     # number of events to skip in the beginning of each step
    d_events  = 1000    # last event number in the step to process
    d_dirmode = 0o774
    d_filemode= 0o664
    d_int_lo  = 1       # lowest  intensity accepted for dark evaluation
    d_int_hi  = 16000   # highest intensity accepted for dark evaluation
    d_intnlo  = 6.0     # intensity ditribution number-of-sigmas low
    d_intnhi  = 6.0     # intensity ditribution number-of-sigmas high
    d_rms_lo  = 0.001   # rms ditribution low
    d_rms_hi  = 16000   # rms ditribution high
    d_rmsnlo  = 6.0     # rms ditribution number-of-sigmas low
    d_rmsnhi  = 6.0     # rms ditribution number-of-sigmas high
    d_fraclm  = 0.1     # allowed fraction limit
    d_fraclo  = 0.05    # fraction of statistics [0,1] below low limit
    d_frachi  = 0.95    # fraction of statistics [0,1] below high limit

    h_fname   = 'input xtc file name, default = %s' % d_fname
    h_exp     = 'experiment name, default = %s' % d_exp
    h_det     = 'detector name, default = %s' % d_det
    h_runs    = 'run number or list of runs e.g. 12,14-18, default = %s' % str(d_runs)
    h_nrecs   = 'number of records to calibrate pedestals, default = %s' % str(d_nrecs)
    h_nrecs1  = 'number of records to process at 1st stage, default = %s' % str(d_nrecs1)
    h_idx     = 'segment index (0-15 for epix10ka2m, 0-3 for quad) or all by default for processing, default = %s' % str(d_idx)
    h_dirxtc  = 'non-default xtc directory, default = %s' % d_dirxtc
    h_dirrepo = 'repository for calibration results, default = %s' % d_dirrepo
    h_usesmd  = 'add "smd" in dataset string, default = %s' % d_usesmd
    h_logmode = 'logging mode, one of %s, default = %s' % (' '.join(DICT_NAME_TO_LEVEL.keys()), d_logmode)
    h_errskip = 'flag to skip errors and keep processing, stop otherwise, default = %s' % d_errskip
    h_stepnum = 'step number to process or None for all steps, default = %s' % str(d_stepnum)
    h_stepmax = 'maximum number of steps to process, default = %s' % str(d_stepmax)
    h_evskip  = 'number of events to skip in the beginning of each step, default = %s' % str(d_evskip)
    h_events  = 'number of events to process from the beginning of each step, default = %s' % str(d_events)
    h_dirmode = 'directory access mode, default = %s' % oct(d_dirmode)
    h_filemode= 'file access mode, default = %s' % oct(d_filemode)
    h_int_lo  = 'lowest  intensity accepted for dark evaluation, default = %d' % d_int_lo
    h_int_hi  = 'highest intensity accepted for dark evaluation, default = %d' % d_int_hi
    h_intnlo  = 'intensity ditribution number-of-sigmas low, default = %f' % d_intnlo
    h_intnhi  = 'intensity ditribution number-of-sigmas high, default = %f' % d_intnhi
    h_rms_lo  = 'rms ditribution low, default = %f' % d_rms_lo
    h_rms_hi  = 'rms ditribution high, default = %f' % d_rms_hi
    h_rmsnlo  = 'rms ditribution number-of-sigmas low, default = %f' % d_rmsnlo
    h_rmsnhi  = 'rms ditribution number-of-sigmas high, default = %f' % d_rmsnhi
    h_fraclm  = 'fraction of statistics [0,1] below low or above high gate limit to assign pixel bad status, default = %f' % d_fraclm
    h_fraclo  = 'fraction of statistics [0,1] below low  limit of the gate, default = %f' % d_fraclo
    h_frachi  = 'fraction of statistics [0,1] above high limit of the gate, default = %f' % d_frachi

    parser = ArgumentParser(description='Proceses dark run xtc data for epix10ka')
    parser.add_argument('-f', '--fname',   default=d_fname,      type=str,   help=h_fname)
    parser.add_argument('-e', '--exp',     default=d_exp,        type=str,   help=h_exp)
    parser.add_argument('-d', '--det',     default=d_det,        type=str,   help=h_det)
    parser.add_argument('-r', '--runs',    default=d_runs,       type=str,   help=h_runs)
    parser.add_argument('-n', '--nrecs',   default=d_nrecs,      type=int,   help=h_nrecs)
    parser.add_argument('--nrecs1',        default=d_nrecs1,     type=int,   help=h_nrecs1)
    parser.add_argument('-i', '--idx',     default=d_idx,        type=int,   help=h_idx)
    parser.add_argument('-x', '--dirxtc',  default=d_dirxtc,     type=str,   help=h_dirxtc)
    parser.add_argument('-o', '--dirrepo', default=d_dirrepo,    type=str,   help=h_dirrepo)
    parser.add_argument('-S', '--usesmd',  action='store_false',             help=h_usesmd)
    parser.add_argument('-L', '--logmode', default=d_logmode,    type=str,   help=h_logmode)
    parser.add_argument('-E', '--errskip', action='store_false',             help=h_errskip)
    parser.add_argument('--stepnum',       default=d_stepnum,    type=int,   help=h_stepnum)
    parser.add_argument('--stepmax',       default=d_stepmax,    type=int,   help=h_stepmax)
    parser.add_argument('--evskip',        default=d_evskip,     type=int,   help=h_evskip)
    parser.add_argument('--events',        default=d_events,     type=int,   help=h_events)
    parser.add_argument('--dirmode',       default=d_dirmode,    type=int,   help=h_dirmode)
    parser.add_argument('--filemode',      default=d_filemode,   type=int,   help=h_filemode)
    parser.add_argument('--int_lo',        default=d_int_lo,     type=int,   help=h_int_lo)
    parser.add_argument('--int_hi',        default=d_int_hi,     type=int,   help=h_int_hi)
    parser.add_argument('--intnlo',        default=d_intnlo,     type=float, help=h_intnlo)
    parser.add_argument('--intnhi',        default=d_intnhi,     type=float, help=h_intnhi)
    parser.add_argument('--rms_lo',        default=d_rms_lo,     type=float, help=h_rms_lo)
    parser.add_argument('--rms_hi',        default=d_rms_hi,     type=float, help=h_rms_hi)
    parser.add_argument('--rmsnlo',        default=d_rmsnlo,     type=float, help=h_rmsnlo)
    parser.add_argument('--rmsnhi',        default=d_rmsnhi,     type=float, help=h_rmsnhi)
    parser.add_argument('--fraclm',        default=d_fraclm,     type=float, help=h_fraclm)
    parser.add_argument('--fraclo',        default=d_fraclo,     type=float, help=h_fraclo)
    parser.add_argument('--frachi',        default=d_frachi,     type=float, help=h_frachi)

    return parser


if __name__ == "__main__":
    do_main()
    sys.exit('End of %s'%SCRNAME)

# EOF
