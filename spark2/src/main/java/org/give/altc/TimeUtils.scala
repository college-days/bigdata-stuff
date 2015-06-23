package org.give.altc

/**
 * Created by zjh on 15-3-24.
 */
object TimeUtils {
    def isLeapYear(year: Int) = {
        if (year % 100 == 0) year % 400 == 0 else year % 4 == 0
    }

    def getDayCountPerYear(year: Int): Int = {
        if (isLeapYear(year)) {
            366
        } else {
            365
        }
    }

    def getDayCountUntilYear(year: Int): Int = {
        var daycount = 0
        for (i <- 0 to year - 1) {
            daycount += getDayCountPerYear(i)
        }
        daycount
    }

    def getDayCountPerMonth(year: Int, month: Int): Int = {
        if (List(1, 3, 5, 7, 8, 10, 12).contains(month)) {
            31
        } else if (List(4, 6, 9, 11).contains(month)) {
            30
        } else {
            if (isLeapYear(year)) {
                29
            } else {
                28
            }
        }
    }

    def getDayCountUntilMonth(year: Int, month: Int): Int = {
        var daycount = 0
        for (i <- 1 to month - 1) {
            daycount += getDayCountPerMonth(year, i)
        }
        daycount
    }

    def getYearMonthDayHour(date: String): (Int, Int, Int, Int) = {
        (date.split("-")(0).toInt, date.split("-")(1).toInt, date.split("-")(2).toInt, date.split("-")(3).toInt)
    }

    def getYearMonthDay(date: String): (Int, Int, Int) = {
        (date.split("-")(0).toInt, date.split("-")(1).toInt, date.split("-")(2).toInt)
    }

    def getHours(date: String): Int = {
        val (year, month, day, hour) = getYearMonthDayHour(date)
        (getDayCountUntilYear(year) + getDayCountUntilMonth(year, month) + day) * 24 + hour
    }

    def getDays(date: String): Int = {
        val (year, month, day) = getYearMonthDay(date)
        getDayCountUntilYear(year) + getDayCountUntilMonth(year, month) + day
    }

    def calcDayDuration(laterDate: String, earlyDate: String): Int = {
        getDays(laterDate) - getDays(earlyDate)
    }

    def calcDateDuration(laterDate: String, earlyDate: String): Int = {
        getHours(laterDate) - getHours(earlyDate)
    }

    //2013-01-07 monday 返回的是星期几的数字
    def getWeekDayFromDate(date: String): Int = {
        val startDate = "2013-01-07"
        val dayDuration = getDays(date) - getDays(startDate)
        dayDuration % 7 + 1
    }
}
