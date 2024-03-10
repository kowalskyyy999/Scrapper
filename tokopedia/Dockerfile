FROM python:3.10.12

RUN mkdir /app

WORKDIR /app 

ADD . .

RUN pip install --upgrade pip && \
    pip install psycopg2-binary && \
    pip install -r requirements.txt

CMD [ "python", "./main.py" ]