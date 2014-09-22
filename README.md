mpich-yarn
===========
#Introduction

MPICH-yarn is an application running on Hadoop YARN that enables
MPI programs running on Hadoop YARN clusters. 

##Prerequisite

As a prerequisite, you need to

1. have a running Hadoop Yarn and HDFS cluster
2. have mpich-3.1.2 deploy to every node of the cluster, and the
binary folder should be in PATH.
3. correctly configure MPICH-yarn.

This version of mpich-yarn uses MPICH-3.1.2 as implementation of MPI
and uses ssh as communication daemon.

##Recommended Configuation

Ubuntu 12.04 LTS
gcc 4.6.3
jdk 1.7.0_25
Apache Maven 3.2.3

#Compile

To compile MPICH-yarn, first you need to have maven installed. Then 
type command at source folder:

	mvn clean package
	
You need to ensure Internet connected as maven needs to download plugins
on the maven repository, this may take minutes.

After this command, you will get mpich2-yarn-1.0-SNAPSHOT.jar at
./target folder. This is the application running at YARN to execute
MPI programs.

#Configuation

There are many tutorials on the Internet about configuring Hadoop, here
is a sample configuration that has successfully run in our cluster.

yarn-site.xml

	<configuration>
	<property>
	    <name>yarn.resourcemanager.resource-tracker.address</name>
	    <value>${YOUR_HOST_IP_OR_NAME}:8031</value>
	  </property>
	  <property>
	    <name>yarn.resourcemanager.address</name>
	    <value>${YOUR_HOST_IP_OR_NAME}:8032</value>
	  </property>
	  <property>
	    <name>yarn.resourcemanager.hostname</name>
	    <value>${YOUR_HOST_IP_OR_NAME}</value>
	  </property>
	  <property>
	    <name>yarn.resourcemanager.scheduler.address</name>
	    <value>${YOUR_HOST_IP_OR_NAME}:8030</value>
	  </property>
	  <property>
	    <name>yarn.resourcemanager.admin.address</name>
	    <value>${YOUR_HOST_IP_OR_NAME}:8033</value>
	  </property>
	  <property>
	    <name>yarn.resourcemanager.webapp.address</name>
	    <value>${YOUR_HOST_IP_OR_NAME}:8088</value>
	  </property>
	<property>
		<name>yarn.nodemanager.aux-services</name>
		<value>mapreduce_shuffle</value>
	</property>
	<property>
		<name>yarn.nodemanager.resource.cpu-vcore</name>
		<value>16</value>
	</property>
	<property>
		<name>yarn.nodemanager.aux-services.mapreduce.shuffle.class</name>
		<value>org.apache.hadoop.mapred.ShuffleHandler</value>
	</property>
	<property>
		<name>yarn.application.classpath</name>
		<value>
			/home/hadoop/hadoop-2.4.1/etc/hadoop,
			/home/hadoop/hadoop-2.4.1/share/hadoop/common/*,
			/home/hadoop/hadoop-2.4.1/share/hadoop/hdfs/*,
			/home/hadoop/hadoop-2.4.1/share/hadoop/yarn/*,
			/home/hadoop/hadoop-2.4.1/share/hadoop/common/lib/*,
			/home/hadoop/hadoop-2.4.1/share/hadoop/hdfs/lib/*,
			/home/hadoop/hadoop-2.4.1/share/hadoop/yarn/lib/*
		</value>
	</property>
	</configuration>
	
mpi-site.conf

	<configuration>
		<property>
			<name>yarn.mpi.scratch.dir</name>
			<value></value>
			<description>
				The HDFS address that stores temporary file like:
				hdfs://sandking04:9000/home/hadoop/mpi-tmp
			</description>
		</property>
		<property>
			<name>yarn.mpi.ssh.authorizedkeys.path</name>
			<value>/home/hadoop/.ssh/authorized_keys</value>
			<description>
				MPICH-YARN will create a temporary RSA key pair for
				password-less login and automatically configure it. 
				All of your hosts should enable public_key login.
			</description>
		</property>
	</configuration> 

#Submit Jobs

## CPI

On the client nodes:

    mpicc -o cpi cpi.c
    hadoop jar mpich2-yarn-1.0-SNAPSHOT.jar -a cpi -M 1024 -m 1024 -n 2

## Hello world

    hadoop jar mpich2-yarn-1.0-SNAPSHOT.jar -a hellow -M 1024 -m 1024 -n 2

## PLDA

    svn checkout http://plda.googlecode.com/svn/trunk/ plda  # Prepare source code
    cd plda
    make  # call mpicc to compile
    cd ..

Put the input data to the hdfs (P.S. there is a testdata in the PLDA source
code dir):

    hadoop fs -mkdir /group/dc/zhuoluo.yzl/plda\_input
    hadoop fs -put plda/testdata/test\_data.txt /group/dc/zhuoluo.yzl/plda\_input/
    hadoop jar mpich2-yarn-1.0-SNAPSHOT.jar -a plda/mpi\_lda -M 1024 -m 1024 -n 2\
     -o "--num_topics 2 --alpha 0.1 --beta 0.01 --training_data_file MPIFILE1 --model_file MPIOUTFILE1 --total_iterations 150"\
     -DMPIFILE1=/group/dc/zhuoluo.yzl/plda_input -SMPIFILE1=true -OMPIOUTFILE1=/group/dc/zhuoluo.yzl/lda_model_output.txt -ppc 2
