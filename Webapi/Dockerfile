# FROM gcr.io/datamechanics/spark:platform-3.1-dm14
# # Install apt dependencies
# ENV PYSPARK_MAJOR_PYTHON_VERSION=3
# # Install requirements
# COPY requirements.txt requirements.txt
# USER root
# # RUN apt-get update -y && apt install build-essential -y
# # RUN pip install --upgrade pip
# RUN pip install -r requirements.txt

# COPY . /app

# EXPOSE 5500
# WORKDIR /app
# ENTRYPOINT python main.py

# FROM apache/spark-py:latest
FROM  jupyter/pyspark-notebook
USER root
RUN pyspark
RUN pip install --upgrade pip

COPY . /app

WORKDIR /app
RUN pip install -r requirements.txt
EXPOSE 5500

# CMD ["/bin/bash", "-c", "spark-submit --master spark://spark-master:7077 /app/modelv2.py"]

ENTRYPOINT python main.py


