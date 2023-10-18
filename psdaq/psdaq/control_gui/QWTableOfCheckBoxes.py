
"""Class :py:class:`QWTableOfCheckBoxes` is derived as QWTableOfCheckBoxes->QWTable->QTableView
===============================================================================================

Usage ::

    # Run test: python lcls2/psdaq/psdaq/control_gui/QWTableOfCheckBoxes.py

    from psdaq.control_gui.QWTableOfCheckBoxes import QWTableOfCheckBoxes
    w = QWTableOfCheckBoxes(tableio=tableio, title_h=title_h, do_ctrl=True, is_visv=True)

    QWTableOfCheckBoxes < QWTable < QTableView

Created on 2019-03-11 by Mikhail Dubrovin
"""

import logging
logger = logging.getLogger(__name__)

from psdaq.control_gui.QWTable import QWTable, QStandardItem, Qt #icon

from re import search as re_search


LIST_STR_CHECK_BOX_STATES = ['UNCHECKED', 'TRISTATE', 'CHECKED']
DICT_CHECK_BOX_STATES = {0:False, 1:False, 2:True}

BIT_CHECKABLE  = 1
BIT_ENABLED    = 2
BIT_EDITABLE   = 4
BIT_SELECTABLE = 8
BIT_HIDDENROW  =16


class QWTableOfCheckBoxes(QWTable):
    """Widget for table with fields containing text with check-boxes."""
    def __init__(self, **kwargs):
        self._list_hidden_rows = []
        QWTable.__init__(self, **kwargs)
        #self._name = self.__class__.__name__
        self.hide_hidden_rows()

    def set_style(self):
        QWTable.set_style(self)

    def fill_table_model(self, **kwargs):
        """Owerrides QWTable.fill_table_model
           tableio is an I/O list of lists, containing str or [bool,str] elements.
           accepts 2-d list of fields: [['just-text', [True,''], [True,'cbx-descr', <int-flag>, "<validator reg.exp.>"]], ...],
           - 'just-text' (str) - field of text
           - [True,''] (list)  - field of check-box
           - [True,'', <int-flag>] (list)  - field of check-box with flags
           - [True,'cbx-descr', <int-flag>, "<validator reg.exp.>"] (list) - field of check-box and text with flags and validator
        after control.getPlatform() request

        """
        self.tableio = kwargs.get('tableio', None)  # I/O 2-d-list table with items "str" or "[bool, str]"
        self.do_live = kwargs.get('do_live', False) # live update of I/O tableio

        title_h = kwargs.get('title_h', None)  # list of horizontal header for cols
        title_v = kwargs.get('title_v', None)  # list of vertical header for rows
        do_ctrl = kwargs.get('do_ctrl', False) # allow to change check-box status
        is_visv = kwargs.get('is_visv', True)  # set visible vertical header, by def [1,2,3,...]
        do_edit = kwargs.get('do_edit', False) # allow to edit text of items
        do_sele = kwargs.get('do_sele', False) # allow selectable

        self.clear_model()
        if title_h is not None: self._si_model.setHorizontalHeaderLabels(title_h)

        self.verticalHeader().setVisible(is_visv)
        if title_v is not None: self._si_model.setVerticalHeaderLabels(title_v)

        self.setFocusPolicy(Qt.NoFocus)

        for row,rec in enumerate(self.tableio):
            row_is_hidden = False
            for col,fld in enumerate(rec):
                if isinstance(fld, list):
                    item = QStandardItem(fld[1])
                    item.setAccessibleDescription('type:list')
                    lsize = len(fld)
                    if lsize==2: # use global table settings
                        item.setCheckState({False:0, True:2}[fld[0]])
                        item.setCheckable (do_ctrl)
                        item.setEnabled   (do_ctrl)
                        item.setEditable  (do_edit)
                        item.setSelectable(do_sele)
                    elif lsize>2: # use field settings depending on flags
                        flags = fld[2]
                        if flags & BIT_CHECKABLE: item.setCheckState({False:0, True:2}[fld[0]])
                        item.setCheckable((flags & BIT_CHECKABLE) and do_ctrl)
                        item.setEnabled   (flags & BIT_ENABLED)
                        item.setEditable ((flags & BIT_EDITABLE) and do_ctrl)
                        item.setSelectable(flags & BIT_SELECTABLE)
                        if flags & BIT_HIDDENROW: row_is_hidden = True

                        if lsize>3: item.valid_reg_exp = fld[3]

                else: # text field
                    item = QStandardItem(fld)
                    item.setAccessibleDescription('type:str')

                #item.setEditable(do_edit)
                self._si_model.setItem(row,col,item)

                #item.setIcon(icon.icon_table)
                #item.setText('Some text')
            if row_is_hidden: self._list_hidden_rows.append(row)

        self.hide_hidden_rows()

    def hide_hidden_rows(self):
        for row in self._list_hidden_rows:
            # 2021-04-29 Chris F. requested to show rows but not editable
            #self.hideRow(row)
            self.set_items_in_hidden_row(row)
            logger.debug('row %d isRowHidden: %s'%(row, self.isRowHidden(row)))
        self.set_exact_widget_size()

    def set_items_in_hidden_row(self, row):
        """ 2021-04-29 Chris F. requested to show rows but not editable.
            this method is used in stead of self.hideRow(row)
        """
        model = self._si_model
        for col in range(model.columnCount()):
            item = model.item(row, col)
            #item.setEditable(False)
            #item.setSelectable(False)
            #item.setCheckable(False)
            item.setEnabled(False)

    def connect_control(self):
        """re-implementation of QWTable.connect_control"""
        #self.connect_item_selected_to(self.on_item_selected)
        self.clicked.connect(self.on_click)
        #self.doubleClicked.connect(self.on_double_click)
        self.connect_item_changed_to(self.on_item_changed)

    def on_item_changed(self, item):
        state = LIST_STR_CHECK_BOX_STATES[item.checkState()]
        index = self._si_model.indexFromItem(item)
        row, col = index.row(), index.column()
        msg = 'on_item_changed: item(%d,%d) name: %s state: %s'%\
              (row, col, item.text(), state)
        logger.debug(msg)

        valid_re = getattr(item, 'valid_reg_exp', None) # search for item.valid_reg_exp
        if valid_re is not None:
            val      = item.text()
            resp = re_search(valid_re, val)
            if resp is None:
                logger.warning('value "%s" IS NOT SET, valid reg.exp.: %s' % (val, valid_re)) # ex: "^([0-9]|1[0-5])$"
                item.setText("N/A")
                return

        if self.do_live:
            if item.accessibleDescription() == 'type:list':
                self.tableio[row][col][0] = DICT_CHECK_BOX_STATES[item.checkState()]
                self.tableio[row][col][1] = str(item.text())
            elif item.accessibleDescription() == 'type:str':
                self.tableio[row][col] = str(item.text())

    def fill_output_object(self):
        """Fills output 2-d list from table of items"""
        model = self._si_model
        list2d_out = []
        for row in range(model.rowCount()):
            list_row = []
            for col in range(model.columnCount()):
                item = model.item(row, col)
                #state = LIST_STR_CHECK_BOX_STATES[item.checkState()]
                #print('item(%d,%d) name: %s state: %s'% (row, col, item.text(), state))
                txt = str(item.text())
                rec = txt if item.accessibleDescription() == 'type:str' else\
                      [DICT_CHECK_BOX_STATES[item.checkState()], txt]
                list_row.append(rec)

            list2d_out.append(list_row)
        return list2d_out


if __name__ == "__main__":
    import sys
    from PyQt5.QtWidgets import QApplication
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.DEBUG)

    app = QApplication(sys.argv)

    title_h = ['str', 'cbx', 'flags']
    tableio = [\
               ['11', [True,  'name 12'], [False, 'name 13aa', 0]],\
               ['21', [False, 'name 22'], [False, 'name 23bb', 1]],\
               ['31', [True,  'name 32'], [False, 'name 33cc', 2]],\
               ['41', [False, 'name 42'], [False, 'name 43dd', 6]],\
               ['51', [True,  'name 52'], [False, 'name 53ee',14]],\
               ['61', [False, 'name 62'], [True,  'name 63ff',15]],\
               ['71', [False, 'name 72'], [True,  'name 73ff',16]],\
               ['81', [True,  'name 82'], [True,  'name 83ff', 6]],\
    ]

    print('%s\nInput table:' % (50*'_'))
    for rec in tableio: print(rec)

    w = QWTableOfCheckBoxes(tableio=tableio, title_h=title_h,\
                            do_ctrl=True, do_live=False, do_edit=True, do_sele=True)
    w.setWindowTitle('QWTableOfCheckBoxes')
    w.move(100,50)
    w.show()
    app.exec_()
    print('%s\nOutput table:' % (50*'_'))
    for rec in w.fill_output_object(): print(rec)
    del w
    del app

# EOF
