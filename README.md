# sql-query-engine

Thesis: Query optimisation in distributed databases

# Manual on how to run on Google Cloud Dataproc's Compute engine

## Set up and configuration

- Create a Google Cloud Storage called storage_intermediate

  - upload the data as a test_data.zip here
  - create a sql-query-engine directory and upload files from this repository there

- Create a Compute Engine Cluster

  - Choose the standard cluster type - (1 master, N workers)
  - Enable component gateway to have access to MapReduce and Yarn web UIs
  - Set up the master node and three worker nodes to n2-standard-2 (2 vCPU, 8 GB)
  - Set up an initialisation action to the script_for_cloud.sh script

- Ssh to all worker nodes and do the following:

  - `sudo chown root /var/lib/hadoop-hdfs`
  - `sudo hdfs --daemon start datanode`

- Ssh to master node from cloud console

  - `cd /home/sql-query-engine/`
  - `source .venv/bin/activate`
  - `export CLUSTER_NAME=<name_of_the_cluster_here>`
  - `export PYSPARK_DRIVER_PYTHON=/home/sql-query-engine/.venv/bin/python`
  - `export PYSPARK_PYTHON=/home/sql-query-engine/.venv/bin/python`
  - `export SPARK_HOME=/usr/lib/spark`
  - `hadoop fs -put /data/* /data/`
  - `sudo yarn --daemon start resourcemanager`
  - `hadoop classpath`
  - edit the property below from `/etc/hadoop/conf/yarn-site.xml` to the values from the command above
  - ```
     <property>
       <name>yarn.application.classpath</name>
       <value>output from hadoop classpath </value>
    </property>
    ```

## Using the application

- `python main.py --env <LOCAL | HDFS> --mode <hadoop | spark> --dd_path <path> <"sql query">`
