import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

/**
 *
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/10/29 17:30 
 */
object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val params = ParameterTool.fromArgs(args)
    val host: String = params.get("host")
    val port: Int = params.getInt("port")

    val textDataSet = env.socketTextStream(host, port)

    val wordCountDataStream = textDataSet.flatMap(_.split(" "))
      .filter(_.nonEmpty)
      .map( (_, 1) )
      .keyBy(0)
      .sum(1)

    wordCountDataStream.print()

    env.execute("stream word count job")
  }
}
