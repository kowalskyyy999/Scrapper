FROM python:3.10-slim

RUN mkdir /app

WORKDIR /app 

ADD . .

RUN pip install -r requirements.txt

CMD [ "python", "./main.py" ]