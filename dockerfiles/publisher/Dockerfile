FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y python3-pip
RUN pip3 install --user confluent-kafka 

COPY publish.py /publish.py

ENTRYPOINT ["python3", "/publish.py"]
