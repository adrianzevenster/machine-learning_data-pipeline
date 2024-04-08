


FROM python:3.10

# DE Project working IDR from DataBase.py
WORKDIR /home/adrian/PycharmProjects/Data_Engineering_Project/DLMSDSEDE02/Docker

COPY requirements.txt ./

RUN pip install -no-cahce-dir -r requirements.txt

copy . .

CMD["python", "./DataBase.py"]
