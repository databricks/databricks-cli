FROM python:3.6

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade pip && \
    pip install -r dev-requirements-py3.txt && \
    pip list && \
    ./lint.sh && \
    pip install . && \
    pytest tests

ENTRYPOINT [ "databricks" ]
