# HDFS namenode + Python 3, so Hadoop Streaming can run Python MapReduce jobs.
# The base image is Debian 8 (jessie), which only offers Python 3.4:
# MapReduce scripts must not use f-strings (Python 3.6+).
FROM bde2020/hadoop-namenode:2.0.0-hadoop2.7.4-java8

# Debian 8 packages now live on the archive server, whose signing key has expired
RUN echo "deb [check-valid-until=no] http://archive.debian.org/debian jessie main" > /etc/apt/sources.list \
 && apt-get -o Acquire::Check-Valid-Until=false update \
 && apt-get install -y --force-yes python3-minimal \
 && rm -rf /var/lib/apt/lists/*
