## steps

* just copy from the [mr project](https://github.com/zjhlocl/news-mr) and import it into the idea

* resolve the maven deps

* just write awesome codes

* compile it into a jar file in the target directory

* move the jar file into the master node

```
scp xxx.jar root@192.168.1.103:xxx
```

please ask me for the password of the master node

* just run it

```
hadoop jar xxx.jar mainclassname inputfilename outputfiledirectory
```

## you should know

* the input and the output file should put in the hdfs

the common file directory is `/user/root`

you can get the files use some hdfs command line

```
hadoop fs -ls /user/root/output
hadoop fs -cat /user/root/output/file
```
