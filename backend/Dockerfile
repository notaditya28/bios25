FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN useradd --create-home --uid 1000 app && mkdir -p /data/cache && chown app /data/cache

# Install dependencies first (cached layer), using a stub package so pip can resolve metadata.
COPY pyproject.toml ./
RUN mkdir app && touch app/__init__.py \
    && pip install --upgrade pip \
    && pip install -e ".[dev]"

COPY . .

USER app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
