package com.zack.apitest.sinktest

import com.zack.apitest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._

/**
 * com.zack.apitest.sinktest
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/11/5 14:28 
 */
object FileTest {
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

    dataStream.print()
    dataStream.writeAsCsv("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\out.txt")

    dataStream.addSink(StreamingFileSink.forRowFormat(
      new Path("D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\out1.txt"),
        new SimpleStringEncoder[SensorReading]())
    .build())

    env.execute("file sink test")
  }
}
