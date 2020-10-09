package com.atguigu.streaming.day02

import java.io.{BufferedReader, InputStreamReader}
import java.net.Socket

import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.ReceiverInputDStream
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Spark02_WordCount_CustomerReceiver {
    def main(args: Array[String]): Unit = {
        // 创建SparkConf
        val sparkConf: SparkConf = new SparkConf().setAppName("Spark02_WordCount_CustomerReceiver$").setMaster("local[*]")
        
        // 初始化SparkSteamingContext
        val ssc = new StreamingContext(sparkConf, Seconds(5))
        
        
        // 从自定义数据源中加载数据创建流
        val lineDStream: ReceiverInputDStream[String] = ssc.receiverStream(new CustomerReceiver("hadoop102", 9999))
        
        //计算并打印
        lineDStream
                .flatMap(_.split("_"))
                .map((_,1))
                .reduceByKey(_+_)
                .print()
        
        //启动SparkStreamingContext
        ssc.start()
        ssc.awaitTermination()
    }
}

//自定义数据源
class CustomerReceiver(host: String, port: Int) extends Receiver[String](StorageLevel.MEMORY_AND_DISK){
    //任务启动时调用的方法
    override def onStart(): Unit = {
        new Thread("socket"){
            override def run(): Unit = {
                //负责接收和保存数据
                receive()
            }
        }.start()
    }
    
    def receive(): Unit ={
        //接收数据
        val socket = new Socket(host,port)
        val reader = new BufferedReader(new InputStreamReader(socket.getInputStream))
        
        var line: String = reader.readLine()
        
        while (isStarted() && line != null) {
            //保存数据
            store(line)
            line = reader.readLine()
        }
        
        reader.close()
        socket.close()
        restart("接收器重启")
        
    }
    
    override def onStop(): Unit = {
        println("Receive Stopped!")
    }
}
