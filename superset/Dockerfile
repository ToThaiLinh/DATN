FROM apache/superset:GHA-19941289755

USER root

ENV ADMIN_USERNAME="admin"
ENV ADMIN_EMAIL="admin@superset.com"
ENV ADMIN_PASSWORD="admin"

COPY ./entrypoint.sh /app/entrypoint.sh

RUN chmod +x /app/entrypoint.sh

COPY superset_config.py /app/
ENV SUPERSET_CONFIG_PATH=/app/superset_config.py

USER superset
ENTRYPOINT [ "/app/entrypoint.sh" ]