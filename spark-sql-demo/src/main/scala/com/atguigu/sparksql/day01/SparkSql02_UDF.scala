package com.atguigu.sparksql.day01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

class SparkSql02_UDF {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql02_UDF").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        
        val df: DataFrame = spark.read.json("./input/people.json")
        
        spark.udf.register("addAge", (age: Long) => age + 5)
        
        df.createOrReplaceTempView("people")
        
        spark.sql("select name,addAge(age) from people").show()
        
        // 关闭SparkSession
        spark.stop()
    }
    
}
