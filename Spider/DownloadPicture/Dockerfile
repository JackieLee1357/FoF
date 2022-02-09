# 拉取基础镜像
FROM python:3.8
 
# 设置环境变量
ENV DEBIAN_FRONTEND=noninteractive
 
# 把当前所有文件拷贝到容器的/code文件夹里，并根据requirements.txt安装python依赖包
RUN mkdir /code \
&& apt-get update -y \
&& apt-get install -y libsm6 \
&& pip install --upgrade pip -i https://pypi.tuna.tsinghua.edu.cn/simple
COPY . /code 
RUN pip install -r /code/requirements.txt -i https://pypi.tuna.tsinghua.edu.cn/simple
WORKDIR /code
 
# 设置时区
RUN ln -sf /usr/share/zoneinfo/Asia/Shanghai /etc/localtime
RUN echo 'Asia/Shanghai' >/etc/timezone
 
CMD ["python3","getBaiduPic.py"]
 