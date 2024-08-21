in this project i developed ETL job using pyspark

after establishing the connection with the source database (MySQL) the ETL job starts

three stages to the etl:

1- extracting the data from the tables in the database to dataframes

2- apply transformation rules to these dataframes and check the rule is valid

3-connect and load the tranformed data into the destination database (Also MySQL, already manually structured)
