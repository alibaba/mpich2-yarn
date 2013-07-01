mpich2-yarn
===========
#Compile

You can use the three ways to get  mpich2-install.tar.gz and mpich2-yarn-1.0-SNAPSHOT.jar.

##Compile the hacked MPICH2

    ./configure --prefix=/home/<USERNAME>/mpich2-install  --with-pm=smpd --with-pmi=smpd
    make
    make install
    tar -zcf mpich2-install.tar.gz /home/<USERNAME>/mpich2-install

##Compile mpich2-yarn

    mvn clean package

##Compile MPICH2 and mpich2-yarn together

    mvn clean package -Dmaven.test.skip=true -DskipMpi=false

This command will generate mpich2-install.tar.gz and mpich2-yarn-1.0-SNAPSHOT.jar together.

#Deploy

##Deploy hacked MPICH2

Distribute mpich2-install.tar.gz to the EVERY node of the cluster including
both client notes and node mangers and extract it.

Client nodes will use the mpich2 for compiling, if you don not want to compile
on the client notes, it is not necessary to put upload the mpich2.

Assuming that the extracted path looks like this:

    /home/<USERNAME>/mpich2-install  # where <USERNAME> can be "hadoop" or something

Make sure the mpich2 is on the PATH of the Nodemanager, ususally we simply add
this line to the yarn-env.sh or the system environment of the Nodemanager:

    export PATH=/path/to/mpich2-install/bin:$PATH

##Deploy mpich2-yarn

Upload mpich2-yarn-1.0-SNAPSHOT.jar to the client nodes.

Assuming that the path looks like this:

    /home/hadoop/mpich2-yarn-1.0-SNAPSHOT.jar

All the ApplicationMaster, Container and Client are packed in this yarn, and it
will be distributed automatically while summiting an MPI Application.

#Configuration

##Client nodes

Configure the following environment:

    export HADOOP_HOME=/home/<USERNAME>/hadoop-current
    export HADOOP_CONF_DIR=/home/<USERNAME>/hadoop-conf
    export MPI_HOME=/home/<USERNAME>/mpich2-install
    export PATH=$MPI_HOME/bin:$HADOOP_HOME:$PATH

Enter $HADOOP\_CONF\_DIF and create mpi-site.xml:

    <?xml version="1.0"?>
    <configuration>
    <property>
      <name>yarn.mpi.scratch.dir</name>
      <value>hdfs://fs.defaultFS:9000/home/username/mpi-tmp</value>
    </property>
    </configuration>

The hdfs path will save the mpi works and appmasters.

##Node Managers

Put the following lines the on the path of the node managers.

    export MPI_HOME=/home/<USERNAME>/mpich2-install
    export PATH=$MP_HOME/bin

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
