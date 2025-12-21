FROM apache/airflow:slim-2.10.0-python3.10

# RUN pip uninstall -y apache-airflow-providers-openlineage

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      procps \
      ca-certificates \
      iputils-ping \
      default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV SPARK_HOME=/opt/spark
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

WORKDIR ${SPARK_HOME}

ENV SPARK_VERSION=3.5.7
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

RUN mkdir -p /home/iceberg/localwarehouse /home/iceberg/notebooks /home/iceberg/warehouse /home/iceberg/spark-events /home/iceberg

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
# Copy custom configuration for master url and events logging
# COPY ./spark-defaults.conf "${SPARK_HOME}/conf"

RUN chown -R airflow ${SPARK_HOME}

USER airflow
WORKDIR /opt/airflow

# Copy the requirements file
COPY requirements.txt .

# Install the dependencies from the requirements file
RUN pip install --no-cache-dir -r requirements.txt

# Copy the DAGs directory to the Airflow home directory
# COPY dags/ /opt/airflow/dags/

# Copy the entrypoint script
COPY entrypoint.sh /entrypoint.sh

# Switch to the root user to change permissions
USER root
RUN chmod +x /entrypoint.sh

# Switch back to the airflow user
USER airflow
ENV PATH="/home/airflow/.local/bin:$PATH"
ENV PATH="${SPARK_HOME}/sbin:${SPARK_HOME}/bin:$PATH"

# Set the entrypoint to the entrypoint script
ENTRYPOINT ["/entrypoint.sh"]