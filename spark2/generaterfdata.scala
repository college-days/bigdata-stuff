val originpath = PathNamespace.prefix + "luolr/traindata"
val outputpath = PathNamespace.prefix + "luorf/traindata"

sc.textFile(originpath).map {
            record =>
                val items = record.split(",")
                val label = items(0)
                val features = items(1).split(" ").zipWithIndex.filter(_._1 != "null").map {
                    record =>
                        (record._2.toInt + 1) + ":" + record._1
                }.mkString(" ")

                label + " " + features
        }.coalesce(1).saveAsTextFile(outputpath)