FROM python:3-slim

RUN pip install -Iv prometheus_client

RUN mkdir /src

ADD parse.py /src/parse.py

CMD ["python","/src/parse.py"]