FROM python:2.7

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade pip && \
    pip install \
        -r dev-requirements.txt \
        -r tox-requirements.txt && \
    pip list && \
    ./lint.sh && \
    pip install . && \
    pytest tests && \
    pip uninstall -y -r tox-requirements.txt

ENTRYPOINT [ "databricks" ]
