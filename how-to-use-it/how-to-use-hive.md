## steps

* clone the [hive project](https://github.com/zjhlocl/news-hive) and import into eclipse

* just write sql to do some simple statical stuff

* you can also checkout the data on the master node

```
hive # get into the shell env
show tables;
select * from tablename;
```

hive just support almost all the operation in standart sql

## you should know

* if you want to load some data into hive 

1. please scp the file to the master node
2. checkout this piece of code [samplecode](https://github.com/zjhlocl/news-hive/blob/master/src/ImportToHive.java#L72)
3. and then you can manipulate these data via sql

* if you want to manipulate the data in hive use mr

1. please load data into the hive first
2. you can findout the data file in `/user/hive/warehouse/` ```hadoop fs -ls /user/hive/warehouse/```
3. now write awesome mr code

