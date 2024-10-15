// ReadTPCDS.scala
import org.apache.spark.sql.{SparkSession, Row}
import org.apache.spark.sql.types.{StructType, StructField, StringType, DoubleType}
import TPCDSQueries._
import scala.collection.mutable.ListBuffer

object RunAllTPCDS {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("TPC-DS Benchmark")
      .enableHiveSupport()
      .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") // 替换为你自己的AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") // 替换为你自己的AWS Secret Key
      .getOrCreate()

    // 定义 S3 基础路径
    val s3BasePath = "s3a://emr-on-eks-nvme-776937568034-us-east-1/BLOG_TPCDS-TEST-100G-partitioned/"
//    val s3BasePath = "s3a://yuanzhe-tpcds-ap/hive_tpcds_ap/"

    // 列出所有 TPC-DS 表
    val tables = Seq(
      "call_center", "catalog_page", "catalog_returns", "catalog_sales", "customer",
      "customer_address", "customer_demographics", "date_dim", "household_demographics",
      "income_band", "inventory", "item", "promotion", "reason", "ship_mode", "store",
      "store_returns", "store_sales", "time_dim", "warehouse", "web_page", "web_returns",
      "web_sales", "web_site"
    )

    // 逐个加载每个表并创建临时视图
    tables.foreach { table =>
      val tablePath = s"$s3BasePath$table/"
      println(s"Loading table: $table from $tablePath")
      spark.read
        .option("basePath", s3BasePath)
        .parquet(tablePath)
        .createOrReplaceTempView(table)
    }

    // 定义要测试的查询数量和迭代次数
    val numQueries = 103
    val numIterations = 1

    // 选择查询
    val queriesToTest = tpcds1_4Queries.take(numQueries)

    // 存储执行时间的列表，列表中的每个元素代表一轮迭代
    val executionTimes = ListBuffer[Map[String, Double]]()

    // 执行SQL并测量执行时间的函数
    def runQuery(queryId: String, query: String): Double = {
      try {
        println(s"Executing query $queryId...")
        val startTime = System.nanoTime()

        val resultDF = spark.sql(query)

        // 触发动作以确保查询执行完成
        resultDF.collect()

        val endTime = System.nanoTime()
        val durationSeconds = (endTime - startTime) / 1e9

        println(f"Query $queryId Execution Time: $durationSeconds%.3f seconds")

        // 展示部分结果
        resultDF.show(10, truncate = false)

        durationSeconds
      } catch {
        case e: Exception =>
          println(s"Query $queryId failed with exception: ${e.getMessage}")
          0.0
      }
    }

    // 进行3次迭代
    for (iteration <- 1 to numIterations) {
      println(s"\n=== Iteration $iteration ===")
      val iterationTime = scala.collection.mutable.Map[String, Double]()
      queriesToTest.foreach { case (queryId, query) =>
        val duration = runQuery(queryId, query)
        iterationTime += (queryId -> duration)
      }
      executionTimes += iterationTime.toMap
      spark.catalog.clearCache() // 清除缓存，避免影响后续查询
    }

    // 创建执行时间的表格
    println("\n=== All Query Execution Times ===")
    // 准备表格的标题
    val header = Seq("Iteration") ++ queriesToTest.map(_._1)
    // 准备表格的内容
    val rows = executionTimes.zipWithIndex.map { case (timeMap, idx) =>
      val iteration = s"${idx + 1}"
      val times = queriesToTest.map { case (queryId, _) =>
        timeMap.getOrElse(queryId, 0.0)
      }
      (iteration +: times).toSeq
    }

    // 定义Schema，"Iteration"为String，其余为Double
    val schema = StructType(
      header.map(fieldName =>
        if (fieldName == "Iteration") StructField(fieldName, StringType, nullable = false)
        else StructField(fieldName, DoubleType, nullable = false)
      )
    )

    // 将rows转换为RDD[Row]
    val rowRDD = spark.sparkContext.parallelize(rows.map { row =>
      Row.fromSeq(
        row.head +: row.tail.map(_.asInstanceOf[Double])
      )
    })

    // 创建DataFrame
    val rowsDF = spark.createDataFrame(rowRDD, schema)

    // 打印表头
    println(header.mkString("\t"))

    // 打印每一行
    rows.foreach { row =>
      println(row.mkString("\t"))
    }

    // 可选：以DataFrame形式展示执行时间表格
    println("\n=== Execution Times DataFrame ===")
    rowsDF.show(false)

    // 可选：将执行时间保存到CSV文件
    rowsDF.coalesce(1).write
      .option("header", "true")
//      .csv("s3a://yuanzhe-tpcds-ap/execution_times_remote.csv")
      .csv("s3://emr-on-eks-nvme-776937568034-us-east-1/execution_times_s3_2.csv")

    // 停止 SparkSession
    spark.stop()
  }
}
