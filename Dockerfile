FROM python:3.8-buster

WORKDIR /app

RUN pip3 install pika && pip3 install sqlalchemy && pip3 install psycopg2-binary

COPY app.py .
COPY database.py .
COPY database_connection.py .
COPY garbage_collector.py .
COPY rabbitmq_connection.py .

CMD ["python3", "app.py"]
