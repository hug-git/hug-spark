package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSql03_UDAF {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql03_UDAF$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        
        spark.udf.register("myAvg", new MyAvg01)
        
        val df: DataFrame = spark.read.json("./input/people.json")
        
        df.createTempView("people")
        
        spark.sql("select myAvg(age) from people").show()
        
        // 关闭SparkSession
        spark.stop()
    }
}

class MyAvg01 extends UserDefinedAggregateFunction {
    //输入数据类型
    override def inputSchema: StructType = StructType(Array(StructField("input", LongType)))
    
    //中间数据类型
    override def bufferSchema: StructType = StructType(Array(
        StructField("sum", LongType),
        StructField("count", IntegerType)
        ))
    
    //输出数据类型
    override def dataType: DataType = DoubleType
    
    //当前函数类型(true为稳定类型，即输入不变，输出则不变)
    override def deterministic: Boolean = true
    
    //缓冲数据初始化
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = 0L
        buffer(1) = 0
    }
    
    //区内数据累加
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
        buffer(0) = buffer.getLong(0) + input.getLong(0)
        buffer(1) = buffer.getInt(1) + 1
    }
    
    //区间合并
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
        buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
        buffer1(1) = buffer1.getInt(1) + buffer2.getInt(1)
    }
    
    //计算函数
    override def evaluate(buffer: Row): Any = {
        Math.round(buffer.getLong(0) * 100D / buffer.getInt(1)) / 100D
    }
}
