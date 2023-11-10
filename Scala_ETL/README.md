# OBJECTIVE: Run ETL (Extract Transfer Load) pipeline on Spark cluster.
The project contains the following steps:
* Get data, in the form of hundreds json files, from the internet. Two different datasets are extracted from the web page 1 and the web page 2, respectively. Each web page has many sub-pages, accessible by adding &page=${i} to URL (**Extract.scala**).
* Transfer hundrends of files to HDFS (**toHDFS.scala**). 
* Convert transfered files to DataFrames and union them, in order to get two DataFrames, corresponding to the web page 1 and the web page 2, respectively (**df_union.scala** ).
* Join these two dataframes.ML pipeline (Scaling + Simple Model), fiting together with prediction (**ETL.scala**). 
* Save the resulting DataFrame to HDFS as .csv file for further usage by data scientist (**ETL.scala**).
> [!NOTE]
> For technical side how to run the code please see the file How_to_run_scala.txt.
