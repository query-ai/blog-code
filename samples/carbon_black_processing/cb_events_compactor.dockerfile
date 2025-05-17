FROM python:3.12-slim AS builder

WORKDIR /app

ENV VENV_PATH=/opt/venv

RUN python -m venv $VENV_PATH && \
    $VENV_PATH/bin/pip install --upgrade pip && \
    $VENV_PATH/bin/pip install boto3 pyarrow

COPY cb_events_compactor.py /app/

FROM python:3.12-slim AS runtime

ENV VENV_PATH=/opt/venv
ENV PATH="$VENV_PATH/bin:$PATH"

COPY --from=builder $VENV_PATH $VENV_PATH
COPY --from=builder /app/cb_events_compactor.py /app/

WORKDIR /app

CMD ["python3", "cb_events_compactor.py"]