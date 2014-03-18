Apache Hadoop Configurations for Savanna
========================================

This directory contains default XML configuration files:

* core-default.xml,
* hdfs-default.xml,
* mapred-default.xml,
* oozie-default.xml,
* hive-default.xml

These files are applied for Savanna's plugin of Apache Hadoop version 1.2.1,
Oozie 4.0.0, Hive version 0.11.0.


Files were taken from here:
https://github.com/apache/hadoop-common/blob/release-1.2.1/src/hdfs/hdfs-default.xml
https://github.com/apache/hadoop-common/blob/release-1.2.1/src/mapred/mapred-default.xml
https://github.com/apache/hadoop-common/blob/release-1.2.1/src/core/core-default.xml
https://github.com/apache/oozie/blob/release-4.0.0/core/src/main/resources/oozie-default.xml
https://github.com/apache/hive/blob/release-0.11.0/conf/hive-default.xml.template

XML configs are used to expose default Hadoop configurations to the users through
the Savanna's REST API. It allows users to override some config values which will
be pushed to the provisioned VMs running Hadoop services as part of appropriate
xml config.
