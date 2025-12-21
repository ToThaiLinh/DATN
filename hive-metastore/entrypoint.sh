#!/bin/sh

export HADOOP_HOME=/opt/hadoop-3.2.0
export HADOOP_CLASSPATH=${HADOOP_HOME}/share/hadoop/tools/lib/aws-java-sdk-bundle-1.11.375.jar:${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-aws-3.2.0.jar
export JAVA_HOME=/usr/local/openjdk-8
export METASTORE_DB_HOSTNAME=${METASTORE_DB_HOSTNAME:-localhost}
PG_PORT=${METASTORE_DB_PORT:-5432}

echo "Waiting for PostgreSQL on ${METASTORE_DB_HOSTNAME}:${PG_PORT} ..."

while ! nc -z ${METASTORE_DB_HOSTNAME} ${PG_PORT}; do
  sleep 1
done

echo "PostgreSQL on ${METASTORE_DB_HOSTNAME}:${PG_PORT} started"

# Check schema
/opt/apache-hive-metastore-3.0.0-bin/bin/schematool -dbType postgres -info
if [ $? -eq 1 ]; then
  echo "Schema not initialized. Initializing..."
  /opt/apache-hive-metastore-3.0.0-bin/bin/schematool -initSchema -dbType postgres
fi

# Start Hive Metastore
/opt/apache-hive-metastore-3.0.0-bin/bin/start-metastore
