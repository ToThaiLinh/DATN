# syntax=docker/dockerfile:1
FROM python:3.10-slim

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends sudo curl unzip default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# RUN pip3 install --no-cache-dir \
#     boto3==1.28.57 \
#     pandas==2.1.1 \
#     pyarrow==13.0.0 \
#     numpy==1.24.3

# Optional env variables
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.8
ENV SPARK_MAJOR_VERSION=3.5
ENV ICEBERG_VERSION=1.9.0
ENV HADOOP_VERSION=3.3.4
ENV AWS_SDK_VERSION=1.12.625

# Download spark
RUN mkdir -p ${SPARK_HOME} \
 && curl https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz -o spark-${SPARK_VERSION}-bin-hadoop3.tgz \
 && tar xvzf spark-${SPARK_VERSION}-bin-hadoop3.tgz --directory /opt/spark --strip-components 1 \
 && rm -rf spark-${SPARK_VERSION}-bin-hadoop3.tgz

# Download iceberg spark runtime
RUN curl https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-spark-runtime-${SPARK_MAJOR_VERSION}_2.12-${ICEBERG_VERSION}.jar

# Download AWS bundle
RUN curl -s https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar -Lo /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar

# Download Hadoop AWS
RUN curl -L -s \
    https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar \
    -o /opt/spark/jars/hadoop-aws-${HADOOP_VERSION}.jar

# Download AWS SDK bundle
RUN curl -L -s \
    https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_VERSION}/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar \
    -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_VERSION}.jar

RUN curl -L -s \
   https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    -o /opt/spark/jars/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar

RUN curl -L -s \
    https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar \
    -o /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar

RUN curl -L -s \
    https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar \
    -o /opt/spark/jars/kafka-clients-3.9.1.jar

RUN curl -L -s \
    https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar \
    -o /opt/spark/jars/commons-pool2-2.11.1.jar

# Install AWS CLI
# RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
#  && unzip awscliv2.zip \
#  && sudo ./aws/install \
#  && rm awscliv2.zip \
#  && rm -rf aws/

# Download MySQL JDBC Driver
ENV MYSQL_CONNECTOR_VERSION=8.0.33

RUN curl -L -s \
    https://repo1.maven.org/maven2/mysql/mysql-connector-java/${MYSQL_CONNECTOR_VERSION}/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar \
    -o /opt/spark/jars/mysql-connector-java-${MYSQL_CONNECTOR_VERSION}.jar

COPY ./conf/spark-defaults.conf /opt/spark/conf
ENV PATH="/opt/spark/sbin:/opt/spark/bin:${PATH}"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

RUN useradd -m -s /bin/bash spark \
    && chown -R spark:spark /opt/spark

# USER spark
WORKDIR /opt/spark