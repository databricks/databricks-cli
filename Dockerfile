FROM ubuntu:16.04

WORKDIR /usr/src/databricks-cli

COPY . .

RUN apt-get update

RUN apt-get install -y python-pip

#RUN pip install --upgrade pip && \
#    pip install -r dev-requirements.txt && \
#    pip list && \
#    ./lint.sh && \
#    pip install . 

#ENTRYPOINT [ "databricks" ]
