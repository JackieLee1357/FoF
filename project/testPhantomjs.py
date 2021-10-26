from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
import time

# 指明phantomjs的执行路径
driver = webdriver.PhantomJS(executable_path=r'D:\PhantomJs\phantomjs-2.1.1-windows\bin\phantomjs.exe')
driver.get("http://17.80.194.7/trace-cdd9b6046dcf57332960.js")

# 方法1：显式给3秒加载时间
time.sleep(3)

# 方法2：让 Selenium 不断地检查某个元素是否存在，以此确定页面是否已经完全加载(需要导入库)
try:
    element = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.ID, "loadedButton")))
finally:
    print(driver.page_source)
    driver.close()

# 获取内容
print(driver.page_source)
#
# driver.close()