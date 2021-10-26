# 这是一个示例 Python 脚本。

# 按 Shift+F10 执行或将其替换为您的代码。
# 按 双击 Shift 在所有地方搜索类、文件、工具窗口、操作和设置。
import sys
from PyQt5.QtWidgets import QApplication, QMainWindow
import cala
from functools import partial

# 访问 https://www.jetbrains.com/help/pycharm/ 获取 PyCharm 帮助


def convert(ui):
    input = ui.leftLine.text()
    result = float(input) * 6.7
    ui.rightLine.setText(str(result))


if __name__ == '__main__':
    app = QApplication(sys.argv)
    MainWindow = QMainWindow()
    ui = cala.Ui_Ui_MainWindow()
    ui.setupUi(MainWindow)
    MainWindow.show()
    ui.convertButton.clicked.connect(partial(convert, ui))
    sys.exit(app.exec_())
