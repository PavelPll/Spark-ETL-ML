# OBJECTIVE: Create and Execute Spark Streaming pipeline (data batch processing) on Spark cluster.
The project contains the following steps:
* Generation of an artificial continuous flow of data (./data_flow). Two alternative options are available:
     - Generate dataflow on master using pandas(python) followed by its continuous transfer to HDFS.
     - Generate the same data flow directly on Spark cluster in the format of HDFS using pyspark(python). 
* Reading data in batches in real time, preprocessing, continuous output batches on screen (./src/main/streamApp.scala). 
* Insert data into a table of a SQL database in real time (./src/main/toSQLstreaming.scala and ./SQL_server_installation.txt).
> [!NOTE]
> For technical side how to run the code please see the file How_to_run.txt.
