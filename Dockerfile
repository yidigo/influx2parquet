FROM rackspacedot/python37
RUN sed -i 's/http:\/\/archive\.ubuntu\.com\/ubuntu\//http:\/\/mirrors\.163\.com\/ubuntu\//g' /etc/apt/sources.list
RUN mkdir /packages
RUN mkdir /packages/logs/
ADD ./ /packages/
ENV PYTHONPATH /packages
RUN pip3 install -r /packages/requirements.txt -i https://pypi.doubanio.com/simple
#ENTRYPOINT ["tail", "-f", "/dev/null"]
ENTRYPOINT ["python3","/packages/main.py"]
