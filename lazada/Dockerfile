FROM python:3.10.12

RUN mkdir /app

WORKDIR /app 

ADD . .

RUN pip install --upgrade pip

RUN pip install psycopg2-binary

RUN pip install -r requirements.txt

CMD [ "python", "./main.py" ]
