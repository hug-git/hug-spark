package com.atguigu.sparksql.day01

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SparkSql06_ReadAndWriter {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val conf: SparkConf = new SparkConf().setAppName("SparkSql06_Read$").setMaster("local[*]")
        // 创建SparkSession
        val spark: SparkSession = SparkSession.builder().config(conf).getOrCreate()
        //导入隐式转换
        
        read(spark)
        
        // 关闭SparkSession
        spark.stop()
    }
    
    def read(spark: SparkSession): Unit ={
        //方式1
        spark.read.json("")
        spark.read.orc("")
        spark.read.parquet("")
        spark.read.csv("")
        spark.read.jdbc("./","table",new Properties())
        //方式2
        spark.read.format("json").load()
        spark.read.format("orc").load()
        spark.read.format("parquet").load()
        spark.read.format("csv").load()
        spark.read.format("jdbc").option("","").option("","").load()
        spark.read.load()//默认parquet
        
    }
    
    def writer(spark: SparkSession): Unit ={
        val df: DataFrame = spark.read.json("./")
        
        //方式1
        df.write.json("")
        //...
        df.write.jdbc("","",new Properties())
        
        //方式2
        df.write.format("json").save("")
        //...
        df.write.format("jdbc").option("","").option("","").save()
        df.write.format("parquet").save("")
        df.write.save("")//默认parquet
        
        //保存方式
        df.write.mode(SaveMode.Append).json("")
        df.write.mode(SaveMode.Ignore).json("")
        df.write.mode(SaveMode.ErrorIfExists).json("")
        df.write.mode(SaveMode.Overwrite).format("json").save("")
        
    }
}
