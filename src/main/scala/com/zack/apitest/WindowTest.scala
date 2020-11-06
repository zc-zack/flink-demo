package com.zack.apitest

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * com.zack.apitest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/6 14:15 
 */
object WindowTest {
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

    val resultStream = dataStream
        .map( data => (data.id, data.temperature) )
        .keyBy(_._1) //按照二元组的第一个元素(id)进行分组
       // .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口
        //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(3)))  //滑动时间窗口
//        .window(EventTimeSessionWindows.withGap(Time.seconds(15)))  //会话窗口
//        .timeWindow(Time.seconds(15))
        .countWindow(10)
    env.execute("window test")
  }
}
