//spark and hdfs
val origin = sc.textFile("/user/root/cleantha.txt") //read from hdfs
val result = origin.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _) //do some operations
val result = origin.flatMap(_.split(" ")).map(word => (word, 1)).reduceByKey(_ + _).map(record => ""+record._1+"\t"+record._2)
result.saveAsTextFile("/user/root/ccc") //save to hdfs just like mapreduce it save as a directory

val users = sc.textFile("/user/hive/warehouse/users/000000_0")
val news = sc.textFile("newsdata.txt") //or /user/root/newsdata.txt
users.count // -> 10000
news.count // -> 116225

users.take(10).foreach(println)

//some transform and actions on RDD

val newsSplits = news.map(line => line.split("\t"))
val newsSplits = news.map(line => line.split("\t")).cache //just cache up the RDD in memory to speed up
newsSplits.map(line => line(0)).take(10).foreach(println) //just get the userid
newsSplits.filter(line => line(0).toInt == 52550).map(line => line(0)).collect.foreach(println) //just use filter to get the target userid
//please have you attention here if you want to get the real value from the lazy operations you should add a action on it forexample the code below may not work as you expected

newsSplits.filter(line => line(0).toInt == 52550).map(line => line(0)).foreach(println) //you should apply the collect on the RDD honey
newsSplits.filter(line => line(0).toInt == 52550).map(line => line(2).toInt).collect
newsSplits.filter(line => line(0).toInt == 52550).map(line => line(2).toInt).collect.size //length do the same job

//manipulate array data
var clea = Array(5, 1, 3, 4, 2)
//=>clea: Array[Int] = Array(5, 1, 3, 4, 2)
scala.util.Sorting.quickSort(clea) //or just import scala.util.Sorting.quickSort
clea //=> res5: Array[Int] = Array(1, 2, 3, 4, 5)
clea(clea.size-2) //=> 获得倒数第二个元素，可以用于提取最近一次浏览新闻记录的前一次记录

users.collect.size //=> 10000
for (userid <- users.collect) println(userid) //note this again if you want to get the Array value in the RDD you should use collect to realize the lazy seq

for(userid <- users.collect){
    //println(userid)
    val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
    scala.util.Sorting.quickSort(visitTimestamps)
    val targetTimestamp = visitTimestamps(visitTimestamps.size-2)
}

//10000 users is too much so we could just take 10 users as a test
val partofusers = users.take(10) //=>Array[String] = Array(75, 111, 195, 719, 930, 985, 1137, 1357, 1393, 1626) return a realized array values not an RDD after take action

for(userid <- partofusers){
    //print(userid+"\t")
    val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
    scala.util.Sorting.quickSort(visitTimestamps)
    val targetTimestamp = visitTimestamps(visitTimestamps.size-2)
    println(userid + " " + targetTimestamp)
}


def getTargetTimestamp(userid:Int) = {
    val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
    scala.util.Sorting.quickSort(visitTimestamps)
    val targetTimestamp = visitTimestamps(visitTimestamps.size-2)
    println(userid + " " + targetTimestamp)
    userid + " " + targetTimestamp
}

val result = sc.parallelize(partofusers.map(userid => getTargetTimestamp(userid.toInt)))
result.saveAsTextFile("/user/root/target")
result.cache().coalesce(1).saveAsTextFile("/user/root/target") //=> 默认savefile会和mapreduce一样将结果分散地存在多个文件中，就像上面这一句写的，要明白这里只是指定的存储结果的hdfs路径，如果希望将所有结果都存成一个文件，需要用这一句代码，需要coalesce(1)一下，用cache可以加快一些速度，但是不知道cache有没有坑
partofusers.map(userid => getTargetTimestamp(userid.toInt))

def getTarget(userid:Int, visitTimestamp:Int) = {
    newsSplits.filter(line => line(0).toInt == userid.toInt && line(2).toInt == visitTimestamp.toInt).map(line => line(0) + "\t" + line(1) + "\t" + line(2) + "\t" + line(3))
}

