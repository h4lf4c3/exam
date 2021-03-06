import java.io.StringReader

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.core.IKSegmenter
import scala.collection.mutable.ListBuffer

object categories {
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
    val ss = SparkSession.builder().config(sc.getConf).getOrCreate()
    // 读取所有数据
    val textFile = sc.textFile(inputFile+"1_content.csv")
    // 进行分词
    val splitText = textFile.map(line => fit(line)).zipWithIndex()
    //splitText.foreach(println)
    val data = ss.createDataFrame(splitText).toDF("features","id")
    // 将60%作为训练集，将40%作为测试集
    val splits = data.randomSplit(Array(0.6,0.4),seed=11L)
    val traing = splits(0)
    val test = splits(1)

    //traing.show(10,false)
    val model = new NaiveBayes().fit(traing)

    val predictions = model.transform(test)
    predictions.show()
  }
  // 分词
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