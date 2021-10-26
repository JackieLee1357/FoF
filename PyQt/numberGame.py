#!/E:\PythonProjects\pythonProject2 python3.8
# -*- coding: utf-8 -*-
# ---
# @Software: PyCharm
# @File: numberGame.py
# @Author: Jackie Lee
# @Institution: Wuxi, Jiangsu, China
# @E-mail: Yuan_li5928@jabil.com
# @Site: 
# @Time: 8月 30, 2021
# ---


# !/usr/bin/python3

# coding = utf-8
from PyQt5.QtCore import pyqtSlot
from PyQt5.QtWidgets import *
import UI
from UI.Ui_Ui_guess_number import Ui_Convertion
from random import randint
import sys


class Action(QMainWindow, Ui_MainWindow):
    """
    Class documentation goes here.

    """

    def __init__(self, parent=None):

        """
        Constructor

        @param parent reference to the parent widget

        @type QWidget

        """

        super(Action, self).__init__(parent)

        self.setupUi(self)

        self.num = randint(1, 100)

        self.show()

    def closeEvent(self, event):

        reply = QMessageBox.question(self, '确认', '确认退出吗', QMessageBox.Yes | QMessageBox.No, QMessageBox.No)

        if reply == QMessageBox.Yes:
            event.accept()
        else:
            event.ignore()

    @pyqtSlot()
    def on_pushButton_clicked(self):

        """
        Slot documentation goes here.
        """
        # TODO: not implemented yet

        guessnumber = int(self.lineEdit.text())

        print(self.num)

        if guessnumber > self.num:

            QMessageBox.about(self, '看结果', '猜大了!')

            self.lineEdit.setFocus()

        elif guessnumber < self.num:

            QMessageBox.about(self, '看结果', '猜小了!')

            self.lineEdit.setFocus()

        else:

            QMessageBox.about(self, '看结果', '答对了!进入下一轮!')

            self.num = randint(1, 100)

            self.lineEdit.clear()

            self.lineEdit.setFocus()


if __name__ == "__main__":
    app = QApplication(sys.argv)

    action = Action()

    sys.exit(app.exec_())