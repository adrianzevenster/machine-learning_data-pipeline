FROM python:3.9-slim

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app
COPY EDA.py /app/

RUN python -m pip install --upgrade pip && \
    pip install \
    pandas \
    pyarrow \
    SQLAlchemy\
    PyMySQL \
    matplotlib \
    seaborn \
    opencv-python-headless \
    scikit-learn \
    mysql-connector-python

COPY . /app

CMD ["python", "EDA.py"]
