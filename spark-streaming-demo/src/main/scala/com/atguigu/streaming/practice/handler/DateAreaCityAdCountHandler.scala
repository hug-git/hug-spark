package com.atguigu.streaming.practice.handler

import java.sql.Connection
import java.text.SimpleDateFormat

import com.atguigu.streaming.practice.bean.Ads_log
import com.atguigu.streaming.practice.utils.JdbcUtil
import org.apache.spark.streaming.dstream.DStream

object DateAreaCityAdCountHandler {
    private val sdf = new SimpleDateFormat("yyyy-MM-dd")
    def saveDateAreaCityAdCountToMysql(filterDStream: DStream[Ads_log]): Unit ={
        val dateAreaCityAdCount: DStream[((String, String, String, String), Int)] = filterDStream.map(log => {
            val dateTime: String = sdf.format(log.timestamp)
            ((dateTime, log.area, log.city, log.adid), 1)
        }).reduceByKey(_ + _)
        
        //写库
        dateAreaCityAdCount.foreachRDD(rdd => {
            
            rdd.foreachPartition(iter =>{
                val connection: Connection = JdbcUtil.getConnection
                iter.foreach{
                    case ((dateTime, area, city, adid), count) => {
    
                        JdbcUtil.executeUpdate(connection,
                            """
                              |insert into area_city_ad_count(dt,area,city,adid,count)
                              |values(?,?,?,?,?)
                              |on duplicate key
                              |update count = count + ?
                              |""".stripMargin,
                            Array(dateTime,area,city,adid,count,count))
                        
                        
                    }
                }
                connection.close()
            })
            
        })
        
    }
}
