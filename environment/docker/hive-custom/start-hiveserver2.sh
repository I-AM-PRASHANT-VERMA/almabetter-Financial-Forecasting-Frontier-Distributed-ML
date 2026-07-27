#!/bin/bash
set -euo pipefail

function wait_for_host_port() {
  local host="$1"
  local port="$2"

  until bash -c "echo > /dev/tcp/${host}/${port}" >/dev/null 2>&1; do
    sleep 3
  done
}

# HiveServer2 should start only after the shared metastore is accepting connections.
wait_for_host_port hive-metastore 9083

# Runtime values are written here so the container uses the Compose environment.
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
    <name>hive.metastore.uris</name>
    <value>thrift://hive-metastore:9083</value>
  </property>
</configuration>
EOF

# Replace the shell with HiveServer2 so Docker receives the service exit status.
exec /opt/hive/bin/hiveserver2 \
  --hiveconf hive.server2.enable.doAs=false \
  --hiveconf hive.metastore.uris=thrift://hive-metastore:9083
