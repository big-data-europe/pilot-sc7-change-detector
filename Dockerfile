###################################
# Dockerfile for Change Detection #
# BigDataEurope			  #
###################################

FROM bde2020/spark-java-template:1.5.1-hadoop2.6

MAINTAINER Giorgos Argyriou <gioargyr@gmail.com>

### IF OVERRIDE:
#ENV SPARK_MASTER_NAME
#ENV SPARK_MASTER_PORT
#ENV SPARK_APPLICATION_JAR_LOCATION /app
###

### OVERRIDE
ENV SPARK_APPLICATION_JAR_NAME changedetector-0.0.1-SNAPSHOT-allinone
ENV SPARK_APPLICATION_MAIN_CLASS tileBased.TileBasedFinal

### ARGS
ENV APP_ARGS_HDFSDIR "hdfs://hdfs:9000"
ENV APP_ARGS_MASTERIMG "/inputimages/image1.zip"
ENV APP_ARGS_SLAVEIMG "/inputimages/image2.zip"
ENV APP_ARGS_RESULT "/inputimages"

### UNKOWN ENVs
ENV HDFS_URL=hdfs://hdfs:9000

ADD changedet.sh /

RUN mkdir /inputimages && \
    chmod +x /changedet.sh

CMD ["/bin/bash", "/changedet.sh"]

