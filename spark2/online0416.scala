val a = sc.textFile(PathNamespace.prefix + "subset/offlinealltestfeatures")

val featureinputpath = PathNamespace.prefix + "subset/offlinealltestfeatures"
val traindatainputpath = PathNamespace.offlinetestlabelsubdata

val a = sc.textFile(featureinputpath)

//train
val allfeatures = sc.textFile(featureinputpath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                (useritemid, features)
        }

//3369
val traindata = sc.textFile(traindatainputpath).map {
            record =>
                val items = record.split(",")
                val userid = items(0)
                val itemid = items(1)
                val label = items(2).toInt
                (userid + "-" + itemid, label)
        }.filter(_._2 == 4).distinct

val b = allfeatures.leftOuterJoin(traindata).map {
            record =>
                val useritem = record._1
                val features = record._2._1
                val label = record._2._2 match {
                    case Some(behavior) => "1"
                    case None => "0"
                }
                (label, features)
        }.map(e => e._1 + "," + e._2)

b.coalesce(1).saveAsTextFile(PathNamespace.prefix + "onlinelrfeature")

//test
val useritemFeaturePath = PathNamespace.prefix + "subset/onlinealltrainfeatures"
val a = sc.textFile(useritemFeaturePath)

val b = sc.textFile(useritemFeaturePath).map {
            record =>
                val items = record.split(",")
                val useritemid = items(0)
                val features = items(1).replace("-", " ")

                useritemid + "," + features
        }

b.coalesce(1).saveAsTextFile(PathNamespace.prefix + "onlinelrtest")
