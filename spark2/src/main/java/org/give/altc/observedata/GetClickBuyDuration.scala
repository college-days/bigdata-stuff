package org.give.altc.observedata

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP

/**
 * Created by zjh on 15-3-24.
 */
object GetClickBuyDuration {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        CommonOP.getClickToBuyDuration(sc, "featuredata", "fromclicktobuyduration")
    }
}
