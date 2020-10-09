package com.atguigu.streaming.practice.handler

import java.sql.Connection
import java.text.SimpleDateFormat

import com.atguigu.streaming.practice.bean.Ads_log
import com.atguigu.streaming.practice.utils.JdbcUtil
import org.apache.spark.streaming.Minutes
import org.apache.spark.streaming.dstream.DStream

object LastHourAdCountHandler {
    private val sdf = new SimpleDateFormat("HH:mm")
    def printLastHourAdCount(filterDStream: DStream[Ads_log]): Unit ={
        val adidTimeToOne: DStream[((String, String), Int)] = filterDStream.map(log => {
            val time: String = sdf.format(log.timestamp)
            ((log.adid, time), 1)
        })
        val adidTimeToOneWindow: DStream[((String, String), Int)] = adidTimeToOne.window(Minutes(60))
        val adidTimeCountWindow: DStream[((String, String), Int)] = adidTimeToOneWindow.reduceByKey(_+_)
    
        val adidToTimeCount: DStream[(String, (String, Int))] = adidTimeCountWindow.map {
            case ((adid, time), count) => {
                (adid, (time, count))
            }
        }
        val adidToTimeGroup: DStream[(String, Iterable[(String, Int)])] = adidToTimeCount.groupByKey()
        
        val adidToTimeSorted: DStream[(String, List[(String, Int)])] = adidToTimeGroup.mapValues(iter => iter.toList.sortWith(_._1< _._1))
        
        adidToTimeSorted.print()
    }
}
