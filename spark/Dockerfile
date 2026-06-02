FROM bitnami/spark:3.5

USER root
RUN pip install --no-cache-dir pyspark==3.5.1

WORKDIR /app
COPY . .

USER 1001
