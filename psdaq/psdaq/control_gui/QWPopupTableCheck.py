
"""
:py:class:`QWPopupTableCheck` - Popup table of str items and/with check-boxes
==================================================================================

Usage::

    # Test: python lcls2/psdaq/psdaq/control_gui/QWPopupTableCheck.py

    # Import
    from psdaq.control_gui.QWPopupTableCheck import QWPopupTableCheck

    # Methods - see test

See:
    - :py:class:`QWPopupTableCheck`
    - `lcls2 on github <https://github.com/slac-lcls/lcls2/psdaq/psdaq/control_gui/>`_.

This software was developed for the LCLS2 project.
If you use all or part of it, please give an appropriate acknowledgment.

Created on 2019-03-29 by Mikhail Dubrovin
"""

import logging
logger = logging.getLogger(__name__)

from PyQt5.QtWidgets import QWidget, QHBoxLayout, QVBoxLayout, QPushButton, QSizePolicy#, QDialog, QGridLayout, QCheckBox, QTextEdit, QLabel,
from PyQt5.QtCore import Qt
from psdaq.control_gui.Styles import style
#from psdaq.control_gui.QWTableOfCheckBoxes import QWTableOfCheckBoxes
from psdaq.control_gui.CGWPartitionTable import CGWPartitionTable
from psdaq.control_gui.CGJsonUtils import get_platform, set_platform, list_active_procs
from psdaq.control_gui.CGDaqControl import daq_control


class QWPopupTableCheck(QWidget):
    """
    """
    def __init__(self, **kwargs):
        parent = kwargs.get('parent', None)
        QWidget.__init__(self, parent)

        self.kwargs = kwargs
        self.list2d_out = []

        win_title = kwargs.get('win_title', None)
        if win_title is not None : self.setWindowTitle(win_title)

        #self.wtab = QWTableOfCheckBoxes(**kwargs)
        self.wtab = CGWPartitionTable(**kwargs)
        #self.make_gui_checkbox()

        self.do_ctrl  = kwargs.get('do_ctrl', True)
        self.do_frame = kwargs.get('do_frame', True)

        #self.but_update = QPushButton('&Update')
        #self.but_cancel = QPushButton('&Cancel')
        self.but_apply  = QPushButton('&Apply')

        #self.but_update.clicked.connect(self.on_but_update)
        #self.but_cancel.clicked.connect(self.onCancel)
        self.but_apply.clicked.connect(self.onApply)

        self.hbox = QHBoxLayout()
        #self.hbox.addWidget(self.but_update)
        #self.hbox.addStretch(1)
        #self.hbox.addWidget(self.but_cancel)
        self.hbox.addStretch(1)
        self.hbox.addWidget(self.but_apply)

        self.vbox = QVBoxLayout()
        self.vbox.addWidget(self.wtab)
        self.vbox.addLayout(self.hbox)
        self.setLayout(self.vbox)

        self.setIcons()
        self.set_style()

    def set_style(self):
        #if not self.do_frame:
        #   self.setWindowFlags(self.windowFlags() | Qt.FramelessWindowHint)
        styleGray = "background-color: rgb(230, 240, 230); color: rgb(0, 0, 0);" # Gray
        #styleTest = "background-color: rgb(100, 240, 200); color: rgb(0, 0, 0);"
        styleDefault = ""
        self.setStyleSheet(styleDefault)

        self.layout().setContentsMargins(0,0,0,0)

        self.setMinimumWidth(100)
        #self.but_update.setFixedWidth(70)
        #self.but_cancel.setFixedWidth(70)
        self.but_apply .setFixedWidth(70)

        #self.but_update.setStyleSheet(styleGray)
        #self.but_update.setFocusPolicy(Qt.NoFocus)
        #self.but_cancel.setFocusPolicy(Qt.NoFocus)
        #self.but_cancel.setStyleSheet(styleGray)
        self.but_apply.setStyleSheet(styleGray)
        self.but_apply.setEnabled(self.do_ctrl)
        self.but_apply.setFlat(not self.do_ctrl)

        self.setSizePolicy(QSizePolicy.MinimumExpanding, QSizePolicy.MinimumExpanding)

        self.wtab.setFixedHeight(self.wtab.height()+2)
        self.setFixedWidth(max(self.wtab.width(),285)+2)

        #self.but_update.setVisible(False)
        #self.but_cancel.setVisible(False)

        #self.setWindowFlags(Qt.Window | Qt.FramelessWindowHint)
        #self.setWindowFlags(Qt.Window | Qt.CustomizeWindowHint)

    def setIcons(self):
        try :
          from psdaq.control_gui.QWIcons import icon
          icon.set_icons()
          #self.but_cancel.setIcon(icon.icon_button_cancel)
          self.but_apply .setIcon(icon.icon_button_ok)
        except : pass

    #def on_but_update(self):
    def update_partition_table(self):
        logger.debug('update_partition_table')
        _, list2d = get_platform() # [[[True,''], 'test/19670/daq-tst-dev02', 'testClient2b'], ...]
        logger.debug('list2d\n',list2d)

        self.kwargs['tableio'] = list2d
        wtab = CGWPartitionTable(**self.kwargs)
        self.vbox.replaceWidget(self.wtab, wtab)
        #self.vbox.removeWidget(self.hbox)
        #self.vbox.addLayout(self.hbox)
        self.wtab.close()
        del self.wtab
        self.wtab = wtab
        self.set_style()


    #def onCancel(self):
    #    logger.debug('onCancel')
    #    self.reject()


    def onApply(self):
        logger.debug('onApply')
        self.list2d_out = self.wtab.fill_output_object()
        #self.accept()

        dict_platf, list2d = get_platform() # [[[True,''], 'test/19670/daq-tst-dev02', 'testClient2b'], ...]
        set_platform(dict_platf, self.list2d_out)

        ## 2019-03-13 caf: If Select->Apply is successful, an Allocate transition should be triggered.
        ## 2020-07-29 caf: The Allocate transition will update the active detectors file, if necessary.

        list2d_active = list_active_procs(self.list2d_out)

        if len(list2d_active)==0:
            logger.warning('NO PROCESS SELECTED!')
        else:
            daq_control().setState('allocated')


    def table_out(self):
        return self.list2d_out


if __name__ == "__main__" :
    import os
    os.environ['LIBGL_ALWAYS_INDIRECT'] = '1'

    import sys
    from PyQt5.QtWidgets import QApplication

    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    app = QApplication(sys.argv)

    title_h = ['sel', 'grp', 'level/pid/host', 'ID']
    tableio = [\
               [[True,  ''], '1', 'drp/123456/drp-tst-dev008', 'cookie_9'],\
               [[True,  ''], '1', 'drp/123457/drp-tst-dev009', 'cookie_1'],\
               [[True,  ''], '1', 'drp/123456/drp-tst-dev008', 'cookie_8'],\
               [[True,  ''], '1', 'drp/123457/drp-tst-dev009', 'cookie_0'],\
               [[False, ''],  '', 'teb/123458/drp-tst-dev001', 'teb1'],\
               [[True,  ''], '1', 'drp/123456/drp-tst-dev008', 'tokie_5'],\
               [[True,  ''], '1', 'drp/123457/drp-tst-dev009', 'tokie_6'],\
               [[True,  ''], '1', 'drp/123456/drp-tst-dev008', 'tokie_8'],\
               [[True,  ''], '1', 'drp/123457/drp-tst-dev009', 'tokie_1'],\
               [[False, ''],  '', 'ctr/123459/drp-tst-acc06',  'control'],\
    ]

    print('%s\nI/O  table:' % (50*'_'))
    for rec in tableio : print(rec)

    w = QWPopupTableCheck(tableio=tableio, title_h=title_h, do_ctrl=True, do_edit=True)
    w.move(200,100)
    w.show()
    app.exec_()
    #resp=w.exec_()
    #logger.debug('resp: %s' % {QDialog.Rejected:'Rejected', QDialog.Accepted:'Accepted'}[resp])
    #for name,state in dict_in.items() : logger.debug('%s checkbox state %s' % (name.ljust(10), state))

    print('%s\nI/O table:' % (50*'_'))
    for rec in tableio : print(rec)

    print('%s\nOutput table from items:' % (50*'_'))
    for rec in w.table_out() : print(rec)

    del w
    del app

# EOF
