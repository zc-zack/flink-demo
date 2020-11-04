import org.apache.flink.api.scala._

/**
 *
 *
 * @author zhangc
 * @version 1.0
 * @create 2020/10/29 17:02 
 */
object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment

    val filePath = "D:\\IdeaProjects\\flink-demo\\src\\main\\resources\\hello.txt"
    val inputDataSet = env.readTextFile(filePath)
    val wordCountDataSet = inputDataSet.flatMap(_.split(" "))
        .map( (_, 1) )
        .groupBy(0)
        .sum(1)
    wordCountDataSet.print()
  }
}
