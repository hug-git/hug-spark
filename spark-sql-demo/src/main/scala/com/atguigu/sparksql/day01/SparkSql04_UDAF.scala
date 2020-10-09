package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

/**
 * DataSet
 */
object SparkSql04_UDAF {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql04_UDAF$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        import spark.implicits._
        
        val df: DataFrame = spark.read.json("./input/people.json")
        
        val ds: Dataset[People] = df.as[People]
        
        val avg0 = new MyAvg02
        val column: TypedColumn[People, Double] = avg0.toColumn
        
        ds.select(column).show()
        
        // 关闭SparkSession
        spark.stop()
    }
}
case class AvgBuffer(var sum: Long, var count: Int)
class MyAvg02 extends Aggregator[People,AvgBuffer,Double]{
    override def zero: AvgBuffer = AvgBuffer(0,0)
    
    override def reduce(b: AvgBuffer, a: People): AvgBuffer = {
        b.sum = b.sum + a.age
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
    
    override def bufferEncoder: Encoder[AvgBuffer] = Encoders.product
    
    override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}

