import java.io.StringReader

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.core.IKSegmenter

import scala.collection.mutable.ListBuffer

object splitWord {
  def main(args: Array[String]): Unit = {
    // 去除日志
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.eclipse.jetty.server").setLevel(Level.WARN)
    // 设置输入输出路径
    val inputFile = "hdfs://192.168.160.30:9000/contentdata/"
    val outputFile = "hdfs://192.168.160.30:9000/output/splitdata/"
    // 设置spark配置
    val conf = new SparkConf().setAppName("wordcount").setMaster("local[4]")
    val sc = new SparkContext(conf)
    // 读取所有数据
    val textFile = sc.textFile(inputFile+"*_content.csv")
    // 进行分词
    val splitText = textFile.flatMap(line => {
      val words = fit(line)
      words.map(word => (word,1))
    }
    )
    val result = splitText.reduceByKey((a,b) => a+b)
    result.saveAsTextFile(outputFile)

  }
  def fit(text: String): List[String] = {
    val sr = new StringReader(text)
    val ik = new IKSegmenter(sr,true)
    val lf = new ListBuffer[String]
    var lex = ik.next()
    while (lex!=null){
      lf.+=(lex.getLexemeText)
      lex = ik.next
    }
    lf.toList
  }
}
