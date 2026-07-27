FROM bde2020/hive:2.3.2-postgresql-metastore

USER root

# Custom entry points wait for dependencies and create the required Hive configuration.
COPY start-metastore.sh /usr/local/bin/start-metastore.sh
COPY start-hiveserver2.sh /usr/local/bin/start-hiveserver2.sh

RUN chmod +x /usr/local/bin/start-metastore.sh /usr/local/bin/start-hiveserver2.sh

