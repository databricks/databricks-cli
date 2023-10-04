FROM python:3.7

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade pip && \
    pip install -r dev-requirements.txt && \
    pip list && \
    ./lint.sh && \
    pip install . && \
    pytest tests

ENTRYPOINT [ "databricks" ]
