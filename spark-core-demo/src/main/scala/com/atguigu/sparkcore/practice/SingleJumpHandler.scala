package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD

object SingleJumpHandler {
    //计算单页访问次数：分母
    def getSinglePageCount(lineRDD: RDD[String], fromPages: Array[Int]): RDD[(String, Int)] = {
        //过滤
        val filterRDD: RDD[String] = lineRDD.filter(line => {
            val arr: Array[String] = line.split("_")
            fromPages.contains(arr(3).toInt)
        })
        
        //转换数据结构 line => (pageId,1)
        val pageId_1: RDD[(String, Int)] = filterRDD.map(line => {
            val arr: Array[String] = line.split("_")
            (arr(3), 1)
        })
        pageId_1.reduceByKey(_ + _)
    }
    
    //计算单跳访问次数：分子
    def getSingleJumpCount(lineRDD: RDD[String], fromToPages: Array[String]): RDD[(String, Int)] = {
        val sessionToPageDt: RDD[(String, (String, String))] = lineRDD.map(line => {
            //转换数据结构，将session作为Key  line => (session,(page,dt))
            val arr: Array[String] = line.split("_")
            (arr(2), (arr(3), arr(4)))
        })
        //按照session分组 (session,(page,dt)) => (session,Iter[(page,dt)...])
        val sessionToPageDtIter: RDD[(String, Iterable[(String, String)])] = sessionToPageDt.groupByKey()
        //组内按照时间排序，去除访问页面，组合成单跳数据并过滤
        val fromToPagesListRDD: RDD[List[String]] = sessionToPageDtIter.map { case (_, iter) =>
            //当前session中访问过的页面
            val pages: List[String] = iter.toList.sortBy(_._2).map(_._1)
            //构建访问单跳
            val fromPages: List[String] = pages.dropRight(1)
            val toPages: List[String] = pages.drop(1)
            val sessionFromToPages: List[String] = fromPages.zip(toPages).map { case (from, to) => s"${from}_$to" }
            //根据要求过滤数据
            sessionFromToPages.filter(fromToPages.contains)
        }
        //压平操作
        val fromToPages_1: RDD[(String, Int)] = fromToPagesListRDD.flatMap(x => x.map((_,1)))
        //计算单跳总数
        fromToPages_1.reduceByKey(_+_)
    }
}
