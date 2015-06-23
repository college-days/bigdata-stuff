package org.give.altc.observedata

import org.apache.spark.{SparkConf, SparkContext}
import org.give.altc.CommonOP

/**
 * Created by zjh on 15-3-24.
 */
object GetBuyCountCategoryWeekday {
    def main(args: Array[String]): Unit = {
        val sc = new SparkContext(new SparkConf().setAppName("cleantha"))
        CommonOP.getBuyCountEachCategoryWeekDay(sc, "featuredata", "categorybuycountweekday")
    }
}
