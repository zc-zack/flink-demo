package com.zack.apitest.sinktest

import java.util

import com.zack.apitest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

/**
 * com.zack.apitest.sinktest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/5 15:21 
 */
object EsSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )

    //定义httphost
    val httpHosts = new util.ArrayList[HttpHost]()
    httpHosts.add(new HttpHost("139.224.115.237", 9200))

    //定义写入es 的 function
    val myEsSinkFunc = new ElasticsearchSinkFunction[SensorReading] {
      override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
        //包装一个Map作为data source
        val dataSource  = new util.HashMap[String, String]()
        dataSource.put("id", t.id)
        dataSource.put("temperature", t.temperature.toString)
        dataSource.put("ts", t.timestamp.toString)


        //创建index request 用于发送http请求
        val indexRequest = Requests.indexRequest()
          .index("sensor")
          .`type`("readingdata")
          .source(dataSource)

        //用index 发送请求
        requestIndexer.add(indexRequest)
      }

    }

    dataStream.addSink(new ElasticsearchSink
      .Builder[SensorReading](httpHosts, myEsSinkFunc)
      .build())

    env.execute("es sink test")
  }
}


