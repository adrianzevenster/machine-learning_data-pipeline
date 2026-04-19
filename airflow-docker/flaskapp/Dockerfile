FROM python:3.9

RUN apt-get update && apt-get install -y default-mysql-client

WORKDIR /app

COPY streamrequirements.txt /app/

RUN pip install --no-cache-dir -r streamrequirements.txt

COPY . /app/

RUN chmod +r /app/RawData.csv

EXPOSE 5000

CMD ["sh", "-c", "python DataBase.py && python streamingestion.py"]