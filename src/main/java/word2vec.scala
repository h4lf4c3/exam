import java.io.StringReader

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.feature.Word2Vec
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.wltea.analyzer.core.IKSegmenter

import scala.collection.mutable.ListBuffer

object word2vec {
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
    val data = ss.createDataFrame(splitText).toDF("content","id")
    //data.show(10,false)
    // 实例化一个word2vec对象，维度为3，mincount为词出现5词才进行向量化
    val word2vec = new Word2Vec()
      .setInputCol("content")
      .setOutputCol("result")
      .setVectorSize(3)
      .setMinCount(5)

    val model = word2vec.fit(data)
    model.getVectors.show(10,false)
    // 显示与质量相近的十个词
    model.findSynonyms("质量",10).show(false)
    // 将如下几个词利用训练好的model，转换为向量
    val testdata = ss.createDataFrame(Seq(
      (List("我","可观","室友","光明"),0),
      (List("人们","游戏","生物"),1),
      (List("画质","精细","期待"),2)
    )).toDF("content","id")
    val result = model.transform(testdata)
    result.show(false)

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