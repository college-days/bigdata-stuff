package org.give.altc

/**
 * Created by zjh on 15-4-19.
 */
object FindCommonPrefix {
    def longestCommonPrefix(strs: List[String]): String = {
        if (strs.size == 0) {
            return ""
        }

        val num: Int = strs.size
        val len: Int = strs(0).size

        for (i <- 0 to len - 1) {
            for (j <- 1 to num - 1) {
                if (i > strs(j).size || strs(j)(i) != strs(0)(i)) {
                    return strs(0).substring(0, i)
                }
            }
        }
        strs(0)
    }

    def findLongestCommonPrefix(strs: List[String]): String = {
        val notEmpty = strs.filter(_ != "")

        if (notEmpty.size == 0) {
            return ""
        }

        if (notEmpty.size == 1) {
            return notEmpty(0)
        }

        //a.combinations(2).foreach(e => println(e(0)))
        val prefixs = (2 to notEmpty.size).map {
            i =>
                val localPrefixs = notEmpty.combinations(i).map(e => longestCommonPrefix(e))
                if (!localPrefixs.isEmpty) {
                    localPrefixs.maxBy(_.size)
                } else {
                    ""
                }
        }
        prefixs.maxBy(_.size)
    }

    def main(args: Array[String]): Unit = {
        val a = List("123", "1234", "333", "123", "aaaa", "aaaaac")
        println(findLongestCommonPrefix(a))
    }
}
