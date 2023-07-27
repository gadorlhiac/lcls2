#!/usr/bin/env python
"""
Created on 2018-02-26 by Mikhail Dubrovin
"""
import os
os.environ['LIBGL_ALWAYS_INDIRECT'] = '1' # get rid of libGL error: unable to load driver: swrast_dri.so

import sys
from psana.graphqt.CMWMain import calibman, logging
from psana.detector.dir_root import DIR_LOG_CALIBMAN

LEVEL_NAMES = ', '.join(list(logging._levelToName.values()))

USAGE = 'Calibration Management GUI\n\n'\
      + 'command examples for app %s\n'%sys.argv[0]\
      + '  calibman\n'
#      + '  calibman -u <username> -p <password>\n'\
#      + '  calibman --host=psdbdev01 --port=9306\n'\
#      + '  calibman --host=psanaphi103 -l DEBUG -L cm-log'


def calibman_gui():
    """Calibration Data Base GUI
    """
    parser = input_argument_parser()
    args = parser.parse_args() # TRICK! this line allows -h or --help potion !!!
    kwargs = vars(args)
    #defs = vars(parser.parse_args([])) # dict of defaults only
    #print("args:",args) #args: Namespace(detector='detector_1234', experiment='exp12345', host='psdbdev01', 
    #print("opts:",opts) #opts: {'host': 'psdbdev01', 'port': 9306,
    #print("defs:",defs) #defs: {'host': 'psdbdev01', 'port': 9306,

    if len(sys.argv) == 1:
        print(80*'_')
        parser.print_help()
        parser.print_usage()
        print(USAGE)
        print(80*'_')

    calibman(**kwargs)


class Constants:

    import psana.pscalib.calib.CalibConstants as cc
    d_user       = cc.USERLOGIN
    d_host       = cc.HOST
    d_port       = cc.PORT
    d_upwd       = ''
    d_experiment = 'exp12345'
    d_detector   = 'detector_1234'
    d_loglevel   = 'INFO'
    d_logdir     = DIR_LOG_CALIBMAN
    d_webint     = True
    d_savecfg    = False

    h_host       = 'DB host, default = %s' % d_host
    h_port       = 'DB port, default = %s' % d_port
    h_user       = 'username to access DB, default = %s' % d_user
    h_upwd       = 'password, default = %s' % d_upwd
    h_experiment = 'experiment name, default = %s' % d_experiment
    h_detector   = 'detector name, default = %s' % d_detector
    h_loglevel   = 'logging level from list (%s), default = %s' % (LEVEL_NAMES, d_loglevel)
    h_logdir     = 'logger directory, if specified the logfile will be saved under this directory, default = %s' % str(d_logdir)
    h_webint     = 'use web-based CLI, default = %s' % d_webint
    h_savecfg    = 'save configuration parameters in file at exit, default = %s' % d_savecfg


def input_argument_parser():
    from argparse import ArgumentParser
    parser = ArgumentParser(description='Calibration Management GUI')
    c = Constants()
    parser.add_argument('-d', '--detector',   default=c.d_detector,   type=str, help=c.h_detector)
    parser.add_argument('-e', '--experiment', default=c.d_experiment, type=str, help=c.h_experiment)
    parser.add_argument('-l', '--loglevel',   default=c.d_loglevel,   type=str, help=c.h_loglevel)
    parser.add_argument('-L', '--logdir',     default=c.d_logdir,     type=str, help=c.h_logdir)
    parser.add_argument('-w', '--webint',     default=c.d_webint, action='store_false', help=c.h_webint)
    parser.add_argument('--host',             default=c.d_host,       type=str, help=c.h_host)
    parser.add_argument('--port',             default=c.d_port,       type=str, help=c.h_port)
    parser.add_argument('-u', '--user',       default=c.d_user,       type=str, help=c.h_user)
    parser.add_argument('-p', '--upwd',       default=c.d_upwd,       type=str, help=c.h_upwd)
    parser.add_argument('-S', '--savecfg',    default=c.d_savecfg, action='store_true', help=c.h_savecfg)
    return parser


if __name__ == "__main__":
    calibman_gui()
    sys.exit(0)

# EOF
