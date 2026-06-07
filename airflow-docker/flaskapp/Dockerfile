FROM python:3.9

RUN apt-get update && apt-get install -y default-mysql-client

WORKDIR /app

COPY streamrequirements.txt /app/

RUN pip install --no-cache-dir -r streamrequirements.txt

COPY . /app/

RUN if [ -f /app/RawData.csv ]; then chmod +r /app/RawData.csv; fi

EXPOSE 5000

CMD ["sh", "-c", "python DataBase.py && gunicorn --bind 0.0.0.0:5000 --workers ${GUNICORN_WORKERS:-2} --threads ${GUNICORN_THREADS:-4} --timeout ${GUNICORN_TIMEOUT:-120} streamingestion:app"]
