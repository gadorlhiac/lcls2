#!/usr/bin/env python

import sys
from time import time

from psana.detector.Utils import info_parser_arguments
from psana.detector.UtilsEpix10kaCalib import deploy_constants
from psana.detector.UtilsEpix10ka import GAIN_MODES_IN
from psana.detector.UtilsEpix import DIR_REPO_EPIX10KA
from psana.detector.UtilsLogging import logging, DICT_NAME_TO_LEVEL, init_stream_handler
logger = logging.getLogger(__name__)

scrname = sys.argv[0].rsplit('/')[-1]

def do_main():

    t0_sec = time()

    parser = argument_parser()

    args = parser.parse_args()
    opts = vars(args)
    defs = vars(parser.parse_args([])) # dict of defaults only

    if len(sys.argv)<3: exit('\n%s\n' % usage())

    assert args.exp is not None,  'WARNING: option "-e <experiment>" MUST be specified.'
    assert args.det is not None,  'WARNING: option "-d <detector-name>" MUST be specified.'
    assert args.runs is not None, 'WARNING: option "-r <run-number(s)>" MUST be specified.'

    init_stream_handler(loglevel=args.logmode)

    logger.debug('%s\nIn epix10ka_deploy_constants' % (50*'_'))
    logger.debug('Command line:%s' % ' '.join(sys.argv))
    logger.info(info_parser_arguments(parser))

    deploy_constants(**opts)

    logger.info('is completed, consumed time %.3f sec' % (time() - t0_sec))


def usage(mode=0):
    if   mode == 1: return 'For specified run or timstamp combine gain constants from repository and deploy them in the calib directory'
    elif mode == 2: return 'Try: %s -h' % scrname
    else: return\
           '\n%s -e <experiment> [-d <detector>] [-r <run-number>] [-L <logging-mode>] [-D] [...]' % scrname\
           + '\nTEST COMMAND:'\
           + '\n  %s -e ueddaq02 -d epixquad -r27 -t 20180910111049 -x /cds/data/psdm/ued/ueddaq02/xtc/ -o ./myrepo -L info -D' % scrname\
           + '\nREGULAR COMMAND:'\
           + '\n  %s -e ueddaq02 -d epixquad -r27 -D -L INFO' % scrname\
           + '\n  %s -e ueddaq02 -d epixquad -r27 -t 396 -o ./work -D # deploys 394-end.data for all calibrations found for runs <= 386' % scrname\
           + '\n  %s -e ueddaq02 -d epixquad -r27 -o ./work -D' % scrname\
           + '\n  %s -e ueddaq02 -d epixquad -r27 -o ./work -D --proc=g --low=0.25 --medium=1 --high=1' % scrname\
           + '\n  %s -e ueddaq02 -d epixquad -r65 -t 60 -DTrue # deploy constants for earlier runs (>=60) of the same experiment' % scrname\
           + '\n  %s -e rixx45619 -d epixhr -r11 -x /cds/home/w/weaver/data/rix/rixx45619/xtc --low=0.0125 --medium=0.3333 --high=1 -D' % scrname\
           + '\n  %s -e rixx45619 -d epixhr -r1 --low=0.512 --medium=13.7 --high=41.0 -D' % scrname\
           + '\n  %s -e rixx45619 -d epixhr -r121 -S mytestdb -D # creates private DB with name cdb_epixhr2x2_000001_mytestdb' % scrname\
           + '\n\n  Try: %s -h' % scrname


def argument_parser():
    #from optparse import OptionParser
    from argparse import ArgumentParser

    d_exp     = None # 'mfxx32516'
    d_det     = None # 'NoDetector.0:Epix10ka.3'
    d_runs    = None # 1021
    d_tstamp  = None # 20180910111049
    d_dirxtc  = None # '/reg/d/psdm/mfx/mfxx32516/scratch/gabriel/pulser/xtc/combined'
    d_dirrepo = DIR_REPO_EPIX10KA
    d_deploy  = False
    d_logmode = 'INFO'
    d_proc    = 'psrg'
    d_paninds = None
    d_high    = None #16.40 for epix10ka
    d_medium  = None #5.466
    d_low     = None #0.164
    d_version = 'V2021-10-05'
    d_run_end = 'end'
    d_comment = 'no comment'
    d_dbsuffix= ''

    h_exp     = 'experiment name, default = %s' % d_exp
    h_det     = 'detector name, default = %s' % d_det
    h_runs    = 'run number for beginning of the validity range or list of comma-separated runs, default = %s' % str(d_runs)
    h_tstamp  = 'non-default time stamp in format YYYYmmddHHMMSS or run number(<10000) for constants selection in repo. '\
                'By default run time is used, default = %s' % str(d_tstamp)
    h_dirxtc  = 'non-default xtc directory which is used to access run start time, default = %s' % d_dirxtc
    h_dirrepo = 'non-default repository of calibration results, default = %s' % d_dirrepo
    h_deploy  = 'deploy constants to the calib dir, default = %s' % d_deploy
    h_logmode = 'logging mode, one of %s, default = %s' % (' '.join(DICT_NAME_TO_LEVEL.keys()), d_logmode)
    h_high    = 'default high   gain ADU/keV, default = %s' % str(d_high)
    h_medium  = 'default medium gain ADU/keV, default = %s' % str(d_medium)
    h_low     = 'default low    gain ADU/keV, default = %s' % str(d_low)
    h_proc    = '(str) keyword for processing of "p"-pedestals, "r"-rms, "s"-status, "g" or "c" - gain or charge-injection gain,'\
              + '  default = %s' % d_proc
    h_paninds = 'comma-separated panel indexds to generate constants for subset of panels (ex.: quad from 2M), default = %s' % d_paninds
    h_version = 'constants version, default = %s' % str(d_version)
    h_run_end = 'last run for validity range, default = %s' % str(d_run_end)
    h_comment = 'comment added to constants metadata, default = %s' % str(d_comment)
    h_dbsuffix= 'suffix of the PRIVATE detector db name to deploy constants, default = %s' % str(d_dbsuffix)

    parser = ArgumentParser(description=usage(1)) #, usage = usage())
    parser.add_argument('-e', '--exp',     default=d_exp,      type=str,   help=h_exp)
    parser.add_argument('-d', '--det',     default=d_det,      type=str,   help=h_det)
    parser.add_argument('-r', '--runs',    default=d_runs,     type=str,   help=h_runs)
    parser.add_argument('-t', '--tstamp',  default=d_tstamp,   type=int,   help=h_tstamp)
    parser.add_argument('-x', '--dirxtc',  default=d_dirxtc,   type=str,   help=h_dirxtc)
    parser.add_argument('-o', '--dirrepo', default=d_dirrepo,  type=str,   help=h_dirrepo)
    parser.add_argument('-L', '--logmode', default=d_logmode,  type=str,   help=h_logmode)
    parser.add_argument(      '--high',    default=d_high,     type=float, help=h_high)
    parser.add_argument(      '--medium',  default=d_medium,   type=float, help=h_medium)
    parser.add_argument(      '--low',     default=d_low,      type=float, help=h_low)
    parser.add_argument('-p', '--proc',    default=d_proc,     type=str,   help=h_proc)
    parser.add_argument('-I', '--paninds', default=d_paninds,  type=str,   help=h_paninds)
    parser.add_argument('-v', '--version', default=d_version,  type=str,   help=h_version)
    parser.add_argument('-R', '--run_end', default=d_run_end,  type=str,   help=h_run_end)
    parser.add_argument('-C', '--comment', default=d_comment,  type=str,   help=h_comment)
    parser.add_argument('-S', '--dbsuffix',default=d_dbsuffix, type=str,   help=h_dbsuffix)
    parser.add_argument('-D', '--deploy',  action='store_true', help=h_deploy)

    return parser


if __name__ == "__main__":
    do_main()
    sys.exit('End of %s'%scrname)

# EOF
