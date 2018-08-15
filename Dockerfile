FROM python:2.7

WORKDIR /usr/src/databricks-cli

COPY . .

RUN pip install --upgrade --no-cache-dir pip &&   \
    pip install --no-cache-dir     \
        -r dev-requirements.txt    \
        -r tox-requirements.txt && \
    pip list && \
    ./lint.sh && \
    pip install --no-cache-dir . && \
    pytest tests  && \
    pip uninstall -y \
        -r tox-requirements.txt

ENTRYPOINT [ "databricks" ]
