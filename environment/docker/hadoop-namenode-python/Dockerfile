FROM bde2020/hadoop-namenode:2.0.0-hadoop3.2.1-java8

USER root

# Hadoop Streaming needs Python available inside the container for the mapper and reducer scripts.
RUN set -eux; \
    sed -i 's|deb.debian.org/debian stretch|archive.debian.org/debian stretch|g' /etc/apt/sources.list; \
    sed -i 's|security.debian.org/debian-security stretch/updates|archive.debian.org/debian-security stretch/updates|g' /etc/apt/sources.list; \
    sed -i '/stretch-updates/d' /etc/apt/sources.list; \
    printf 'Acquire::Check-Valid-Until "false";\n' > /etc/apt/apt.conf.d/99no-check-valid-until; \
    apt-get update; \
    apt-get install -y --no-install-recommends python3 python3-minimal; \
    ln -sf /usr/bin/python3 /usr/local/bin/python; \
    rm -rf /var/lib/apt/lists/*

ENV PATH="/usr/local/bin:${PATH}"
