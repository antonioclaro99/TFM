FROM python:3.10

# Configurar repositorio de Microsoft e instalar dependencias
RUN apt-get update && \
    apt-get install -y gnupg2 && \
    curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - && \
    curl https://packages.microsoft.com/config/debian/10/prod.list > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update

# Instalar las dependencias necesarias, incluyendo libodbc y el controlador ODBC
RUN apt-get install -y unixodbc unixodbc-dev && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql17 && \
    rm -rf /var/lib/apt/lists/*
COPY ./requirements.txt /scrapy/requirements.txt

RUN pip install -r /scrapy/requirements.txt

RUN apt update

COPY ./lesiones_transfermarket_redis /scrapy

COPY ./spiders.py /usr/local/lib/python3.10/site-packages/scrapy_redis/spiders.py

WORKDIR /scrapy