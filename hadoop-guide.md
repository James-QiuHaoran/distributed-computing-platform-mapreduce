## Install Hadoop

1. Prerequisite

- Install Java
- Install `ssh` and `pdsh` if needed

2. Download a stable Hadoop release

```
wget https://archive.apache.org/dist/hadoop/core/hadoop-2.7.7/hadoop-2.7.7.tar.gz
tar -xvf hadoop-2.7.7.tar.gz
```

3. Make changes to `.bashrc`

```
export JAVA_HOME=/usr
export HADOOP_HOME=$HOME/hadoop-2.7.7
export HADOOP_CONF_DIR=$HOME/hadoop-2.7.7/etc/hadoop
export HADOOP_MAPRED_HOME=$HOME/hadoop-2.7.7
export HADOOP_COMMON_HOME=$HOME/hadoop-2.7.7
export HADOOP_HDFS_HOME=$HOME/hadoop-2.7.7
export YARN_HOME=$HOME/hadoop-2.7.7
export PATH=$PATH:$HOME/hadoop-2.7.7/bin
export HADOOP_CLASSPATH=/usr/lib/jvm/java-openjdk/lib/tools.jar
```

4. Make the changes work by executing `source .bashrc`

5. Try `java -version` and `hadoop version` to check whether it works

## Cluster Deployment

1. Copy the master node's ssh key (create one if there's none) to slave's authorized keys.

```
ssh-copy-id -i $HOME/.ssh/id_rsa.pub username@slave
```

2. Create master files in `etc/hadoop/masters` and add master's IP to it.

3. Add slave IPs in `etc/hadoop/slaves`.

4. Edit `core-site.xml` on both master and slave machines as follows:

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>fs.default.name</name>
    <value>hdfs://master-ip:9000</value>
</property>
</configuration>
```

5. Edit `hdfs-site.xml` on the master machine as follows:

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>dfs.replication</name>
    <value>2</value>
</property>
<property>
    <name>dfs.permissions</name>
    <value>false</value>
</property>
<property>
    <name>dfs.namenode.name.dir</name>
    <value>/home/haoranq4/hadoop-2.7.7/namenode</value>
</property>
<property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/haoranq4/hadoop-2.7.7/datanode</value>
</property>
</configuration>
```

6. Edit `hdfs-site.xml` on slave machines as follows:

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>dfs.replication</name>
    <value>2</value>
</property>
<property>
    <name>dfs.permissions</name>
    <value>false</value>
</property>
<property>
    <name>dfs.datanode.data.dir</name>
    <value>/home/haoranq4/hadoop-2.7.7/datanode</value>
</property>
</configuration>
```

7. Copy `mapred-site` from the template in configuration folder and the edit `mapred-site.xml` on both master and slave machines as follows:

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
    <name>mapreduce.framework.name</name>
    <value>yarn</value>
</property>
</configuration>
```

8. Edit `yarn-site.xml` on both master and slave machines as follows:

```
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
<property>
　　<name>yarn.resourcemanager.address</name>
　　<value>master:8032</value>
</property>
<property>
　　<name>yarn.resourcemanager.scheduler.address</name>
　　<value>master:8030</value>
</property>
<property>
　　<name>yarn.resourcemanager.resource-tracker.address</name>
　　<value>master:8031</value>
</property>
<property>
　　<name>yarn.resourcemanager.admin.address</name>
　　<value>master:8033</value>
</property>
<property>
　　<name>yarn.resourcemanager.webapp.address</name>
　　<value>master:8088</value>
</property>
<property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
</property>
<property>
    <name>yarn.nodemanager.auxservices.mapreduce.shuffle.class</name>
    <value>org.apache.hadoop.mapred.ShuffleHandler</value>
</property>
</configuration>
```

9. Format the namenode (**only** on the master machine).

```
hadoop namenode -format
```

10. Start all daemons (**only** on the master machine).

```
[haoranq4@master hadoop-2.7.7]$ ./sbin/start-dfs.sh
[haoranq4@master hadoop-2.7.7]$ ./sbin/start-yarn.sh
```

11. Check all the daemons running on both master and slave machines.

```
jps
```

On the master machine, you should see something like this:

```
20869 SecondaryNameNode
21206 NodeManager
20553 NameNode
22620 Jps
32093 Server
21069 ResourceManager
20703 DataNode
```

On the slave machine, you should see something like this:

```
1173 Jps
1500 Server
17134 DataNode
1054 NodeManager
```

## Stop the Service (in the end if needed) (**only** on the master machine).

```
[haoranq4@master hadoop-2.7.7]$ ./sbin/stop-dfs.sh
[haoranq4@master hadoop-2.7.7]$ ./sbin/stop-yarn.sh
```

## Run Word Count Example

```
hadoop fs -mkdir -p /test/input
hadoop fs -put test-files/input-folder /test/input
hadoop fs -ls /test/input
hadoop jar applications/wc-hadoop.jar wordcount /test/input /test/output
hadoop fs -ls /test/output
hadoop fs -get /test/output/part-r-00000 output-folder/output.txt
```

## Run Reverse Web Link Example

```
hadoop fs -mkdir -p /test/input
hadoop fs -put test-files/input-folder /test/input
hadoop fs -ls /test/input
hadoop jar applications/rwlg-hadoop.jar ReverseWebLink /test/input /test/output
hadoop fs -ls /test/output
hadoop fs -get /test/output/part-r-00000 output-folder/output.txt
```

## Compile Your Own Application

```
bin/hadoop com.sun.tools.javac.Main ReverseWebLink.java -> ReverseWebLink*.class
jar cf rwlg.jar ReverseWebLink*.class -> rwlg.jar
```

Follow the above instructions to execute your `.jar` applications on Hadoop!