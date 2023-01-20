FROM ubuntu:20.04

RUN apt-get update
RUN apt-get install -y python3-pip
RUN pip3 install --user confluent-kafka 

COPY create.py /create.py

ENTRYPOINT ["python3", "create.py"]
