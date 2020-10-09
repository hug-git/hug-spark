package com.atguigu.streaming.practice.app

import java.text.SimpleDateFormat
import java.util.{Date, Properties}

import com.atguigu.streaming.practice.bean.Ads_log
import com.atguigu.streaming.practice.handler.{BlackListHandler, DateAreaCityAdCountHandler, LastHourAdCountHandler}
import com.atguigu.streaming.practice.utils.{MyKafkaUtil, PropertiesUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealTimApp {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("RealTimApp$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        val properties: Properties = PropertiesUtil.load("config.properties")
        
        val kafkaStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(properties.getProperty("kafka.topic"), ssc)
    
    
        val adsLogDStream: DStream[Ads_log] = kafkaStream.map(record => {
            val arr: Array[String] = record.value().split(" ")
            //封装样例类对象
            Ads_log(arr(0).toLong, arr(1), arr(2), arr(3), arr(4))
        })
        adsLogDStream.cache()
        adsLogDStream.count().print()
        
        //根据黑名单过滤数据
        val filterAdsLogDStream: DStream[Ads_log] = BlackListHandler.filterByBlackList(adsLogDStream,ssc.sparkContext)
        filterAdsLogDStream.cache()
        filterAdsLogDStream.count().print()
        
        //计算当前批次中，当天某个用户对于某个广告的点击次数
        val sdf = new SimpleDateFormat("yyyy-MM-dd")
        val dateUserAdToCountDStream: DStream[((String, String, String), Int)] = filterAdsLogDStream.map(log => {
            val timestamp: Long = log.timestamp
            val date: String = sdf.format(new Date(timestamp))
            
            //封装元组
            ((date, log.userid, log.adid), 1)
        }).reduceByKey(_ + _)
        
        //将点击次数写入MySQL并作黑名单判断和设置
        BlackListHandler.saveBlackListToMysql(dateUserAdToCountDStream)
        
        //需求二:计算每天各地区各城市各广告的点击总流量，并将其存入MySQL
        DateAreaCityAdCountHandler.saveDateAreaCityAdCountToMysql(filterAdsLogDStream)
        
        //需求三:最近一小时广告点击量,按分钟展示
        LastHourAdCountHandler.printLastHourAdCount(filterAdsLogDStream)
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}
