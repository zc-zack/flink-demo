package com.zack.apitest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

import scala.util.Random

/**
 * com.zack.apitest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/2 10:19 
 */

//温度传感器样例
case class SensorReading(id: String, timestamp: Long, temperature: Double)

object SourceTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //1.从自定义的集合中读取数据
    val dataList = List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.101067604893444)
    )
    val stream1 = env.fromCollection(dataList)

    //2.从文件中读取数据
    val stream2 = env.readTextFile("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt")

//    stream2.print("stream2")
//    stream1.print("stream1").setParallelism(1)

    //3.从kafka中读取数据
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.1.52:9092")
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")

    val stream3 = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )

    //stream3.print()


    //4.自定义source

    val stream4 = env.addSource(new MySensorSource);
    stream4.print()

    env.execute("source test")
  }
}

//自定义source function

class MySensorSource() extends SourceFunction[SensorReading] {

  //定义一个标志位，用来表示数据源是否正常运行发出数据
  var running: Boolean = true

  override def run(sourceContext: SourceFunction.SourceContext[SensorReading]): Unit = {
    //定义一个随机数生成器
    val rand = new Random()

    //随机生成一组（10个）传感器的初始温度(id, temp)
    var currentTemp = 1.to(10).map(i => ("sensor_" + i, rand.nextDouble() * 100))

    // 定义无限循环，不停地产生数据，除非被cancel
    while(running) {
      //在上次数据基础上微调更新温度值
      currentTemp = currentTemp.map(
        data => (data._1, data._2 + rand.nextGaussian())
      )
      //获取当前的时间戳，加入到数据中
      val currentTime = System.currentTimeMillis();
      currentTemp.foreach(data =>sourceContext.collect(SensorReading(data._1, currentTime, data._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
