FROM python:3.11.5

RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 psycopg2-binary python-dotenv

WORKDIR /app

COPY data_ingestion.py .env ./

ENTRYPOINT [ "python3", "data_ingestion.py"]