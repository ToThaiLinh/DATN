# check=skip=UndefinedVar
FROM bitsondatadev/hive-metastore:latest

USER root

RUN sed -i 's|http://deb.debian.org/debian|http://archive.debian.org/debian|g' /etc/apt/sources.list && \
    sed -i '/deb.*security.debian.org/s/^/#/' /etc/apt/sources.list && \
    apt-get update && \
    apt-get install -y wget netcat && \
    rm -rf /var/lib/apt/lists/*

RUN wget https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.7/postgresql-42.7.7.jar \
    -O /opt/apache-hive-metastore-3.0.0-bin/lib/postgresql-42.7.7.jar

# RUN wget https://repo1.maven.org/maven2/org/apache/logging/log4j/log4j-web/2.8.2/log4j-web-2.8.2.jar \
#     -O /opt/apache-hive-metastore-3.0.0-bin/lib/log4j-web-2.8.2.jar

COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT [ "/entrypoint.sh" ]

EXPOSE 9083

USER $NB_UID
