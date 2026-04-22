#!/bin/bash
set -euo pipefail

function wait_for_host_port() {
  local host="$1"
  local port="$2"

  until bash -c "echo > /dev/tcp/${host}/${port}" >/dev/null 2>&1; do
    sleep 3
  done
}

wait_for_host_port namenode 9870
wait_for_host_port hive-metastore-db 5432

cat > /opt/hive/conf/hive-site.xml <<EOF
<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>${HIVE_CORE_CONF_javax_jdo_option_ConnectionURL}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>${HIVE_CORE_CONF_javax_jdo_option_ConnectionDriverName}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>${HIVE_CORE_CONF_javax_jdo_option_ConnectionUserName}</value>
  </property>
  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>${HIVE_CORE_CONF_javax_jdo_option_ConnectionPassword}</value>
  </property>
  <property>
    <name>hive.metastore.warehouse.dir</name>
    <value>/user/hive/warehouse</value>
  </property>
  <property>
    <name>hive.metastore.schema.verification</name>
    <value>false</value>
  </property>
</configuration>
EOF

export HADOOP_CLASSPATH="${HADOOP_CLASSPATH:-}:$HIVE_HOME/lib/*"

# These directories are required by Hive before the service starts answering queries.
/opt/hadoop-2.7.4/bin/hdfs dfs -mkdir -p /tmp || true
/opt/hadoop-2.7.4/bin/hdfs dfs -mkdir -p /user/hive/warehouse || true
/opt/hadoop-2.7.4/bin/hdfs dfs -chmod g+w /tmp || true
/opt/hadoop-2.7.4/bin/hdfs dfs -chmod g+w /user/hive/warehouse || true

# Schema initialization only needs to succeed once, so an already-initialized schema is fine.
/opt/hive/bin/schematool \
  -dbType postgres \
  -initSchema \
  -userName "${HIVE_CORE_CONF_javax_jdo_option_ConnectionUserName}" \
  -passWord "${HIVE_CORE_CONF_javax_jdo_option_ConnectionPassword}" || true

exec /opt/hive/bin/hive --service metastore \
  --hiveconf hive.metastore.uris=thrift://0.0.0.0:9083
