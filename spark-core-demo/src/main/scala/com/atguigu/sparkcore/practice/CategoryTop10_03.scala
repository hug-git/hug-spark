package com.atguigu.sparkcore.practice

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object CategoryTop10_03 {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("CategoryTop10_03$").setMaster("local[*]")
        // 创建SparkContext
        val sc: SparkContext = new SparkContext(conf)
    
        //过滤掉“搜索”行为
        val originalData: RDD[String] = sc.textFile("./input/user_visit_action.txt")
        originalData.cache()
        
        //创建并注册累加器
        val accum: MyAccumulator = new MyAccumulator
        sc.register(accum)
        
        case0(originalData, accum)
        
        // 关闭SparkContext
        sc.stop()
        
    }
    
    
    def case0(originalData: RDD[String], accum: MyAccumulator): Unit = {
        originalData.foreach(line => {
            val arr: Array[String] = line.split("_")
            arr match {
                case click if click(6).toInt != -1 => accum.add(click(6), (1, 0, 0))
                case order if order(8) != "null" => order(8).split(",").foreach(accum.add(_, (0, 1, 0)))
                case pay if pay(10) != "null" => pay(10).split(",").foreach(accum.add(_, (0, 0, 1)))
                case _ => Nil
            }
        })
        accum.value.toList.sortBy(_._2).reverse.take(10).foreach(println)
    }
}

case class MyAccumulator() extends AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] {
    private val map: mutable.Map[String, (Int, Int, Int)] = mutable.Map[String, (Int, Int, Int)]()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]] = new MyAccumulator
    
    override def reset(): Unit = map.clear()
    
    /**
     * 根据品类Id和操作进行累加
     *
     * @param v (categoryId,action)
     */
    override def add(v: (String, (Int, Int, Int))): Unit = {
        val categoryId: String = v._1
        val tuple1: (Int, Int, Int) = map.getOrElse(categoryId, (0, 0, 0))
        val tuple2: (Int, Int, Int) = v._2
        map(categoryId) = (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
    }
    
    override def merge(other: AccumulatorV2[(String, (Int, Int, Int)), mutable.Map[String, (Int, Int, Int)]]): Unit = {
        other.value.foreach(
            kv => {
                val categoryId: String = kv._1
                val tuple1: (Int, Int, Int) = map.getOrElse(categoryId, (0, 0, 0))
                val tuple2: (Int, Int, Int) = kv._2
                map(categoryId) = (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
            })
    }
    
    override def value: mutable.Map[String, (Int, Int, Int)] = map
}

/**
 * Accumulator1
 */
case class MyAccumulator1() extends AccumulatorV2[(String, String), mutable.Map[String, (Int, Int, Int)]] {
    private val map: mutable.Map[String, (Int, Int, Int)] = mutable.Map[String, (Int, Int, Int)]()
    
    override def isZero: Boolean = map.isEmpty
    
    override def copy(): AccumulatorV2[(String, String), mutable.Map[String, (Int, Int, Int)]] = new MyAccumulator1
    
    override def reset(): Unit = map.clear()
    
    /**
     * 根据品类Id和操作进行累加
     *
     * @param v (categoryId,action)
     */
    override def add(v: (String, String)): Unit = {
        val categoryId: String = v._1
        val action: String = v._2
        val tuple: (Int, Int, Int) = map.getOrElse(categoryId, (0, 0, 0))
        map(categoryId) = action match {
            case "click" => (tuple._1 + 1, tuple._2, tuple._3)
            case "order" => (tuple._1, tuple._2 + 1, tuple._3)
            case "pay" => (tuple._1, tuple._2, tuple._3 + 1)
            case _ => throw new RuntimeException("Illegal Action")
        }
    }
    
    override def merge(other: AccumulatorV2[(String, String), mutable.Map[String, (Int, Int, Int)]]): Unit = {
        other.value.foreach(
            kv => {
                val categoryId: String = kv._1
                val tuple1: (Int, Int, Int) = map.getOrElse(categoryId, (0, 0, 0))
                val tuple2 = kv._2
                map(categoryId) = (tuple1._1 + tuple2._1, tuple1._2 + tuple2._2, tuple1._3 + tuple2._3)
            })
    }
    
    override def value: mutable.Map[String, (Int, Int, Int)] = map
}
