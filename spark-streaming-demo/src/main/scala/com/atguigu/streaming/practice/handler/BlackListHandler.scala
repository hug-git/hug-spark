package com.atguigu.streaming.practice.handler

import java.sql.Connection

import com.atguigu.streaming.practice.bean.Ads_log
import com.atguigu.streaming.practice.utils.JdbcUtil
import org.apache.spark.SparkContext
import org.apache.spark.streaming.dstream.DStream

object BlackListHandler {
    /**
     * 根据黑名单过滤数据
     * @param adsLogDStream 原始数据
     * @param sc SprkContext
     * @return 过滤后的数据
     */
    def filterByBlackList(adsLogDStream: DStream[Ads_log], sc: SparkContext): DStream[Ads_log] ={
        //使用mapPartitions代替filter，减少连接的创建与释放
        adsLogDStream.mapPartitions(iter => {
            val filterIter: Iterator[Ads_log] = iter.filter(log => {
                val connection: Connection = JdbcUtil.getConnection
                val bool: Boolean = JdbcUtil.isExist(connection,
                    "select userid from black_list where userid=?",
                    Array(log.userid))
                connection.close()
                ! bool
            })
            filterIter
        })
    }
    
    def saveBlackListToMysql(dateUserAdToCountDStream: DStream[((String,String,String),Int)]): Unit = {
        dateUserAdToCountDStream.foreachRDD(rdd => {
            //分区操作
            rdd.foreachPartition(iter => {
                val connection: Connection = JdbcUtil.getConnection
                //遍历区内数据
                iter.foreach{
                    case ((date,user,ad),ct) => {
                        // 将当前批次的数据结合MySQL中已有的数据写出
                        JdbcUtil.executeUpdate(connection,
                        """
                              |INSERT INTO user_ad_count(dt,userid,adid,count)
                              |VALUES (?,?,?,?)
                              |ON DUPLICATE KEY
                              |UPDATE count = count + ?;
                              |""".stripMargin,
                            Array(date,user,ad,ct,ct))
                        
                        //读取用户对于某个广告的单日点击总次数
                        val userAdCount: Long = JdbcUtil.getDataFromMysql(connection,
                            """
                              |SELECT
                              |  count
                              |FROM
                              |  user_ad_count
                              |WHERE
                              |  dt=? and userid=? and adid=?;
                              |""".stripMargin,
                            Array(date, user, ad))
                        
                        //如果点击总次数超过100，则将用户写入黑名单(MySQL)
                        if (userAdCount >= 100) {
                            JdbcUtil.executeUpdate(connection,
                            """
                                  |INSERT INTO black_list(userid)
                                  |VALUE(?)
                                  |ON DUPLICATE KEY
                                  |UPDATE userid=?
                                  |""".stripMargin,
                                Array(user,user))
                        }
                    }
                }
                
                connection.close()
            })
        })
    }
    
    def main(args: Array[String]): Unit = {
        val connection: Connection = JdbcUtil.getConnection
        val bool: Boolean = JdbcUtil.isExist(connection,
            "select userid from black_list where userid=?",
            Array("2"))
        println(bool)
    }
}
