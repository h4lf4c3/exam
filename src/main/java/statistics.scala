import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession




object statistics{
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


    // 定义游戏类型数组
    var gameType = Array[String]("策略","第一人称射击","动作","格斗","横版过关","即时战略","角色扮演","竞速","乱斗/清版","冒险","模拟","射击","体育","益智","音乐旋律")
    // 定义一个countType变量，记录当前类型游戏的总数，用于统计类型数最多的游戏
    var countType:Map[String,Long] = Map()

    // 循环读取目录下每个文件
    for(i <- 1 to 15) {
      // 读取dataframe
      val df = ss.read.format("csv").load(inputFile+i+".csv")
      // 修改列名
      val schema = Seq("id","name","star","rating","platforms","n_ratings","genres","content","type")
      val dfRename = df.toDF(schema: _*)
      //dfRename.select(dfRename("n_ratings").cast(DoubleType)).show()
      // 统计最多点赞游戏
      //dfRename.select(dfRename("name"),dfRename("n_ratings").cast(DoubleType)).sort(dfRename("n_ratings").desc).show()
      System.out.println("点赞最多游戏：")
      dfRename.select(dfRename("name"),dfRename("n_ratings"))
        .sort(dfRename("n_ratings").desc)
        .show(10)

      //统计游戏个数
      System.out.println("点赞数超过100人的游戏个数："+dfRename.filter(dfRename("n_ratings") > 100).count())
      //dfRename.filter(dfRename("n_ratings") > 100).count()

      // 统计哪个游戏平台最多
      System.out.println("最多的游戏平台：")
      val plat = dfRename.groupBy(dfRename("platforms")).count()
      plat.sort(plat("count").desc).show(5)

      // 统计收藏数最多的游戏
      System.out.println("收藏数最多的游戏：")
      dfRename.select(dfRename("name"),dfRename("star"))
        .sort(dfRename("star").desc)
        .show(10)

      // 当前游戏类型数添加到数组中
      countType += (gameType(i-1)->dfRename.count())

    }
    System.out.println("最多的游戏类型是")
    countType.toArray.sortWith(_._2 > _._2).foreach(println)
  }
}