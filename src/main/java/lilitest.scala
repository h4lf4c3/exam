import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession



object lilitest{
  def main(args: Array[String]): Unit = {
    // 去除日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    // 设置输入输出路径
    val inputFile = "hdfs://192.168.160.30:9000/data/"
    val outputFile = "hdfs://192.168.160.30:9000/output/"
    // 设置spark配置
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 构建session对象处理csv
    val ss = SparkSession.builder().config(sc.getConf).getOrCreate()
      // 读取dataframe
    val df = ss.read.format("csv").load(inputFile+"homework.csv")
      // 修改列名
    val schema = Seq("user_name","user_id","age","sex","locate","content","content_id","rating","time")
    val dfRename = df.toDF(schema: _*)
      // 查看模式，以确保重命名了
    dfRename.printSchema()
    dfRename.select("content").show(20,30)
//    dfRename.select("content").write.format("csv")
//      .save(outputFile+"home_content.csv")
//    dfRename.select("content").write.format("csv")
//      .save(outputFile+i+"_content.csv")
    }
}