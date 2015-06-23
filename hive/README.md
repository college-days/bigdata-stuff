## how to use this project

just import it into eclipse
and put the jar files into the build path of this project

## how to use hive

just put yourself in the situation that just manipulate the data in mysql/postgresql or some other database like this via jdbc

## what can hive do

join, group by, union, and almost all common operation in standard sql

## what the eggache part

just can not insert data into tables like the way used to do it, if you want to init some data insert into hive table, you should put the file on hdfs and load data from hdfs into hive, and than you can manipulate the data use sql instead of writing mapreduce job

## notice

if you want to use hive, please let me known so i can start the hiveserver to enable the connection via jdbc
