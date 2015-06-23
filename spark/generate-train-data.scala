val users = sc.textFile("/user/hive/warehouse/users/000000_0")
val news = sc.textFile("/user/hive/warehouse/usernews/newsdata.txt") //or /user/root/newsdata.txt

val newsSplits = news.map(line => line.split("\t"))

//获得最近第二次的浏览时间
def getTargetTimestamp(userid:Int) = {
    val visitTimestamps = newsSplits.filter(line => line(0).toInt == userid.toInt).map(line => line(2).toInt).collect
    scala.util.Sorting.quickSort(visitTimestamps)
    val targetTimestamp = visitTimestamps(visitTimestamps.size-2)
    println(userid + " " + targetTimestamp)
    Map("userid" -> userid, "timestamp" -> targetTimestamp)
}

//根据获取到的浏览时间和用户id来得到用户id对应的训练样本记录
def getTarget(userid:Int, visitTimestamp:Int) = {
    println(userid + " " + visitTimestamp)
    newsSplits.filter(line => line(0).toInt == userid.toInt && line(2).toInt == visitTimestamp.toInt).map(line => line(0) + "\t" + line(1) + "\t" + line(2) + "\t" + line(3)).collect
}

//all in one
//最终hdfs文件中存储的格式为userid newsid visit_timestamp visit_time
def generateTrainPositive(users:Array[String], savePath:String) = {
    val targetUserTimestamp = users.map(userid => getTargetTimestamp(userid.toInt))
    val result = targetUserTimestamp.flatMap(usertimepair => getTarget(usertimepair("userid").toInt, usertimepair("timestamp").toInt)) //=> don't forget to use flatMap to merge all the sub Array into one Array
    val finalResult = sc.parallelize(result) //=> don't forget convert Array to a RDD here
    finalResult.cache().coalesce(1).saveAsTextFile(savePath)
}

//this is just a tes use small dataset and it really worked
/*val partofusers = users.take(10)
//this code is just take about 15 seconds
val targetUserTimestamp = partofusers.map(userid => getTargetTimestamp(userid.toInt))
val result = targetUserTimestamp.flatMap(usertimepair => getTarget(usertimepair("userid").toInt, usertimepair("timestamp").toInt)) //=> don't forget to use flatMap to merge all the sub Array into one Array
val finalResult = sc.parallelize(result) //=> don't forget convert Array to a RDD here
finalResult.cache().coalesce(1).saveAsTextFile("/user/root/target1")*/

//this test is really worked
generateTrainPositive(users.take(10), "/user/root/target1")

//start to get train positive data
generateTrainPositive(users.collect, "/user/root/trainpositive")

//validation
val trainpositive = sc.textFile("/user/root/trainpositive/part-00000")
trainpositive.count

//http://stackoverflow.com/questions/24729544/scala-find-duplicate-elements-in-a-list
//http://stackoverflow.com/questions/5778657/how-do-i-convert-an-arraystring-to-a-setstring

trainpositive.map(line => line.split("\t")).map(line => line(0)).collect.toSet.size //=> 10000
trainpositive.map(line => line.split("\t")).map(line => line(0)).collect.size //=> 10078
trainpositive.map(line => line.split("\t")).map(line => line(0))
trainpositive.map(line => line.split("\t")).map(line => line(0)).groupBy(identity).collect { case (x,ys) if ys.size > 1 => x }.collect //find duplicate

trainpositive.map(line => line.split("\t")).map(line => (line(0), line(2))).collect.toSet.size //=> 10000
trainpositive.map(line => line.split("\t")).map(line => (line(0), line(2))).collect.size //=> 10078
trainpositive.map(line => line.split("\t")).map(line => (line(0), line(2))).groupBy(identity).collect { case (x,ys) if ys.size > 1 => x }.collect //find duplicate
trainpositive.map(line => line.split("\t")).map(line => (line(0), line(2))).groupBy(identity).collect { case (x,ys) if ys.size > 2 => x }.collect //find duplicate

trainpositive.map(line => line.split("\t")).map(line => (line(0), line(1), line(2))).collect.toSet.size //=> 10000
trainpositive.map(line => line.split("\t")).map(line => (line(0), line(1), line(2))).collect.size //=> 10078
trainpositive.map(line => line.split("\t")).map(line => (line(0), line(1), line(2))).groupBy(identity).collect { case (x,ys) if ys.size > 1 => x }.collect //find duplicate

trainpositive.map{ line =>
    line.split("\t")
}
