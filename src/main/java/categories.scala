import com.hjj.common.IK
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
object categories {


  /**
    * 贝叶斯分类
    *
    *
    * 1、对数据进行处理，去除脏数据
    * 2、对评论进行分词
    * 3、将数据转换成向量，加上tf-idf（评价一个词相对于文档的重要程度）
    * 4、将训练集带入贝叶斯算法  训练模型
    * 5、计算模型准确率
    * 6、将模型保存到hdfs
    *
    *
    */

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("train")
      .setMaster("local[4]")
      .set("spark.sql.shuffle.partitions", "4")
    // 设置输入路径
    val inputFile = "hdfs://192.168.160.30:9000/contentdata/"

    val sc = new SparkContext(conf)

    val textFile = sc.textFile(inputFile+"*_content.csv")
    val data = textFile.map(line => "0\t"+line)
    //1、脏数据过滤
    val filterRDD = data
      .map(_.split("\t"))
      .map(arr => (arr(0), arr(1)))
      .map(t => (t._1, t._2.replace("'", "")))
      .map(t => (t._1, "http.*".r.replaceAllIn(t._2, "")))
      .map(t => (t._1, "#".r.replaceAllIn(t._2, "")))
      .map(t => (t._1, "�".r.replaceAllIn(t._2, "")))
      .map(t => (t._1, "\\s".r.replaceAllIn(t._2, "")))
      .map(t => (t._1, "[a-zA-Z0-9]".r.replaceAllIn(t._2, "")))
      //.map(t => (t._1, "\\p{P}".r.replaceAllIn(t._2, " ")))
      .map(t => (t._1, "【".r.replaceAllIn(t._2, " ")))
      .filter(t => !t._2.contains("转发微博"))
      .filter(t => t._2.trim.length > 1)


    //2，对数据进行分词
    val wordsRDD = filterRDD.map(t => {
      (t._1.toDouble, IK.fit(t._2))
    })
      .filter(_._2.length > 1)

    //3、将数据转换成向量，加上tf-idf

    val sql = new SQLContext(sc)
    import sql.implicits._

    //将RDD转换成DF
    val srcDF = wordsRDD
      .map(t => (t._1, t._2.mkString(" ")))
      .toDF("label", "text")


    //Tokenizer  英文分词器
    val tok = new Tokenizer()
      .setInputCol("text")
      .setOutputCol("feature")

    val tokDF = tok.transform(srcDF)


    //计算tf  词频
    val tfModel = new HashingTF()
      .setInputCol("feature")
      .setOutputCol("tf")

    val tfDF = tfModel.transform(tokDF)


    //计算idf

    //计算if-idf
    val idf = new IDF()
      .setInputCol("tf")
      .setOutputCol("features")

    //训练idf模型
    val idfModel = idf.fit(tfDF)

    val tfIdfDF = idfModel.transform(tfDF)


    //tfIdfDF.show(100, false)


    //切分训练集和测试集
    val splitDF = tfIdfDF.randomSplit(Array(0.8, 0.2))
    val trainDF = splitDF(0)
    val testDF = splitDF(1)

    //构建贝叶斯算法
    val naiveBayes = new NaiveBayes()
      .setLabelCol("label")
      .setFeaturesCol("features")
      .setModelType("multinomial")

    //训练模型，  贝叶斯算法的模型是一概率
    val nbModel = naiveBayes.fit(trainDF)

    //通过测试集进行测试
    val redultDF = nbModel.transform(testDF)
    redultDF.cache()

    redultDF.show(false)

    //计算模型准确率
    val flagRDD = redultDF
      .rdd
      .map(row => {
        //预测结果
        val prediction = row.getAs[Double]("prediction")
        //原始结果
        val label = row.getAs[Double]("label")
        math.abs(prediction - label)
      })

    //模型准确率：
    val testaccuracy = 1 - flagRDD.sum() / flagRDD.count().toDouble

    println("模型准确率：" + testaccuracy)
    //保存模型
    if (testaccuracy > 0.8) {
      //保存模型
      idfModel.write.overwrite().save("model/idfModel")
      nbModel.write.overwrite().save("model/nbModel")
    }
  }
}
