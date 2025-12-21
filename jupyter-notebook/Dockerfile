FROM python:3.10.19-slim

USER root

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
      sudo \
      curl \
      vim \
      unzip \
      ca-certificates \
    #   iputils-ping \
      default-jdk && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip3 install -r requirements.txt

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

# Add a notebook command
# RUN echo '#! /bin/sh' >> /bin/notebook \
#  && echo "export PYSPARK_DRIVER_PYTHON=jupyter-notebook" >> /bin/notebook \
#  && echo "PATH=/opt/spark/sbin:/opt/spark/bin:$PATH" >> /bin/notebook \
#  && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --NotebookApp.token='' --NotebookApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/notebook \
#  && echo "pyspark" >> /bin/notebook \
#  && chmod u+x /bin/notebook

# RUN echo '#! /bin/sh' >> /bin/jupyterlab \
#  && echo 'export PYSPARK_DRIVER_PYTHON=jupyter-lab' >> /bin/jupyterlab \
#  && echo "PATH=/opt/spark/sbin:/opt/spark/bin:$PATH" >> /bin/jupyterlab \
#  && echo "export PYSPARK_DRIVER_PYTHON_OPTS=\"--notebook-dir=/home/iceberg/notebooks --ip='*' --LabApp.token='' --LabApp.password='' --port=8888 --no-browser --allow-root\"" >> /bin/jupyterlab \
#  && echo "pyspark" >> /bin/jupyterlab \
#  && chmod +x /bin/jupyterlab

COPY spark-defaults.conf /opt/spark/conf
ENV PATH="/bin:/usr/local/bin:usr/local/sbin:/opt/spark/sbin:/opt/spark/bin"

RUN chmod u+x /opt/spark/sbin/* && \
    chmod u+x /opt/spark/bin/*

CMD ["/bin/sh", "-c", "/usr/local/bin/notebook --notebook-dir=/home/iceberg/notebooks --ip=* --LabApp.token= --LabApp.password= --no-browser --allow-root"]