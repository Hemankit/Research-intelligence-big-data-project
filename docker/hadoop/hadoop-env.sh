# docker/hadoop/hadoop-env.sh
# Mounted into the NameNode and DataNode containers.
# Sets JVM memory limits

export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export HADOOP_HEAPSIZE=512          
export HADOOP_NAMENODE_INIT_HEAPSIZE=512
