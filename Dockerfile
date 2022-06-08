FROM python:2.7

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade pip && \
    pip install -r dev-requirements-py2.txt && \
    pip list && \
    ./lint.sh && \
    pip install . && \
    pytest tests

ENTRYPOINT [ "databricks" ]
