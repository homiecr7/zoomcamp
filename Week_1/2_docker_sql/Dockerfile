# base image
FROM python:3.9

# runs this command in bash
RUN apt-get install wget
RUN pip install pandas sqlalchemy psycopg2 pyarrow

# wroking directory in the container
WORKDIR /app

# copies the files from source system to working directory in the container
COPY injest.py injest.py

# will prompt bash
ENTRYPOINT [ "python", "injest.py"]