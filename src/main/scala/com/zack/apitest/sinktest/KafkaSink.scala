package com.zack.apitest.sinktest

import com.zack.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internal.FlinkKafkaProducer

/**
 * com.zack.apitest.sinktest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/5 14:49 
 */
object KafkaSink {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
    val inputStream = env.readTextFile(inputPath)

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble).toString
      } )

    dataStream.addSink(new FlinkKafkaProducer011[String]("192.168.1.52:9092", "sinkTest", new SimpleStringSchema()))

    //从kafka读取数据

    env.execute("kafka sink test")
  }
}
