FROM python:2.7

WORKDIR /usr/src/databricks-cli

COPY dev-requirements.txt .

RUN pip install --upgrade pip && \
    pip install -r dev-requirements.txt

COPY . .

RUN ./lint.sh && \
    pip install . && \
    pytest tests

ENTRYPOINT [ "databricks" ]
