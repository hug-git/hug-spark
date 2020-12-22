package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * DataFrame
 */
object SparkSql05_UDAF {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql05_UDAF$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        
        val df: DataFrame = spark.read.json("./input/people.json")
        spark.udf.register("myAvg", functions.udaf(new MyAvg03))
        
        df.createTempView("people")
        
        spark.sql("select myAvg(age) from people").show()
        
        // change by huguo
        // 关闭SparkSession
        spark.stop()
    }
}

class MyAvg03 extends Aggregator[Long, AvgBuffer,Double]{
    override def zero: AvgBuffer = AvgBuffer(0,0)
    
    override def reduce(b: AvgBuffer, a: Long): AvgBuffer = {
        b.sum = b.sum + a
        b.count = b.count + 1
        b
    }
    
    override def merge(b1: AvgBuffer, b2: AvgBuffer): AvgBuffer = {
        b1.sum = b1.sum + b2.sum
        b1.count = b1.count + b2.count
        b1
    }
    
    override def finish(reduction: AvgBuffer): Double = {
        Math.round(reduction.sum * 100D / reduction.count) / 100D
    }
    
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product[]
    
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}