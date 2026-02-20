FROM python:3.11.9-slim AS builder

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    VENV_PATH=/opt/venv

WORKDIR /build

RUN python -m venv "${VENV_PATH}"
ENV PATH="${VENV_PATH}/bin:${PATH}"

COPY requirements.txt /build/requirements.txt
RUN pip install --upgrade pip setuptools wheel \
    && pip install --prefer-binary -r /build/requirements.txt

FROM python:3.11.9-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    VENV_PATH=/opt/venv \
    PORT=8000

WORKDIR /app

RUN addgroup --system appgroup && adduser --system --ingroup appgroup appuser

COPY --from=builder /opt/venv /opt/venv
ENV PATH="${VENV_PATH}/bin:${PATH}"

COPY app /app/app
COPY sample_data /app/sample_data

EXPOSE 8000

USER appuser

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT}"]
