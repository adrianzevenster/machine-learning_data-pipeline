FROM python:3.9-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY requirements.txt /app/

RUN python -m pip install --upgrade pip && \
    pip install -r requirements.txt

COPY . /app

CMD ["python", "EDA.py"]
