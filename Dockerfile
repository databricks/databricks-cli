FROM python:2.7

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade pip && \
    pip install -r dev-requirements.txt && \
    ./lint.sh && \
    pip install . && \
    pytest tests

ENTRYPOINT [ "databricks" ]
