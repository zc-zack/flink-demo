package com.zack.apitest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
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
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

//    val inputPath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\sensor.txt"
//    val inputStream = env.readTextFile(inputPath)

    val inputStream = env.socketTextStream("192.168.1.52", 7777);

    val dataStream = inputStream
      .map( data => {
        val arr = data.split(",")
        SensorReading(arr(0).trim, arr(1).trim.toLong, arr(2).trim.toDouble)
      } )
    //  .assignAscendingTimestamps(_.timestamp * 1000L)\
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(3)) {
        override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000L
      })

    val lateTag = new OutputTag[(String, Double, Long)]("late")
    // 每15秒统计一次，窗口内各传感器的温度值的最小值
    val resultStream = dataStream
        .map( data => (data.id, data.temperature, data.timestamp) )
        .keyBy(_._1) //按照二元组的第一个元素(id)进行分组
       // .window(TumblingEventTimeWindows.of(Time.seconds(15))) //滚动时间窗口
        //.window(SlidingEventTimeWindows.of(Time.seconds(15), Time.seconds(3)))  //滑动时间窗口
//        .window(EventTimeSessionWindows.withGap(Time.seconds(15)))  //会话窗口
//        .timeWindow(Time.seconds(15))
       // .countWindow(10)  //滚动计数窗口
        .timeWindow(Time.seconds(15))
        .allowedLateness(Time.minutes(1))
        .sideOutputLateData(lateTag)
        .reduce( (currentResult, newData) => (currentResult._1, currentResult._2.min(newData._2), newData._3) )

    resultStream.getSideOutput(lateTag).print("late")
    resultStream.print("result")
    env.execute("window test")
  }
}

class MyReducer extends ReduceFunction[SensorReading] {
  override def reduce(t: SensorReading, t1: SensorReading): SensorReading = {
    SensorReading(t.id, t1.timestamp, t.temperature.min(t1.temperature))
  }
}
