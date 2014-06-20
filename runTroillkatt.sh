# Tip. Use hadoop classpath: to get the necessary hadoop jars
#

MY_JAVA_LIB=/home/epe005/troilkatt/java
TROILKATT_HOME=/home/epe005/troilkatt/troilkatt

#java -Xmx2048m -classpath `hbase classpath`:$MY_JAVA_LIB/commons-cli.jar:$MY_JAVA_LIB/mongo-2.10.1.jar:$MY_JAVA_LIB/commons-compress.jar:$MY_JAVA_LIB/commons-logging.jar:$MY_JAVA_LIB/commons-io.jar:$MY_JAVA_LIB/commons-net.jar:$MY_JAVA_LIB/gnu-getopt.jar:$HBASE_CONF:$MY_JAVA_LIB/log4j-1.2.jar:$MY_JAVA_LIB/move.jar:$HADOOP_CONF:$HBASE_CONF:$TROILKATT_HOME/bin edu.princeton.function.troilkatt.Troilkatt -c conf/epe.xml -d conf/pipelines -l log4j.properties -s recovery

java -Xmx2048m -classpath `hbase classpath`:$MY_JAVA_LIB/commons-cli.jar:$MY_JAVA_LIB/mongo-2.10.1.jar:$MY_JAVA_LIB/commons-compress.jar:$MY_JAVA_LIB/commons-logging.jar:$MY_JAVA_LIB/commons-io.jar:$MY_JAVA_LIB/commons-net.jar:$MY_JAVA_LIB/gnu-getopt.jar:$HBASE_CONF:$MY_JAVA_LIB/log4j-1.2.jar:$MY_JAVA_LIB/move.jar:$HADOOP_CONF:$HBASE_CONF:$TROILKATT_HOME/bin edu.princeton.function.troilkatt.fs.FSTests


#java -Xmx2048m -classpath $TROILKATT_HOME/bin:`hbase classpath`:$MY_JAVA_LIB/commons-cli.jar:$MY_JAVA_LIB/mongo-2.10.1.jar:$MY_JAVA_LIB/commons-compress.jar:$MY_JAVA_LIB/commons-logging.jar:$MY_JAVA_LIB/commons-io.jar:$MY_JAVA_LIB/commons-net.jar:$MY_JAVA_LIB/gnu-getopt.jar:$HBASE_CONF:$MY_JAVA_LIB/log4j-1.2.jar edu.princeton.function.troilkatt.clients.GetLogFiles -c conf/epe.xml -l log4j.properties -t 1388665958069 -mr hefalmp-geo-gds-human 001-remove_overlapping /home/epe005/troilkatt/tmp
