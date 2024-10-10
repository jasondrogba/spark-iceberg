import org.apache.spark.sql.SparkSession

object ReadTPCDS {
  def main(args: Array[String]): Unit = {
    // 初始化 SparkSession
    val spark = SparkSession.builder()
      .appName("TPCDS Spark SQL Runner")
      .getOrCreate()

    // 设置 S3 配置（如果需要）
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    // 根据需要设置更多 S3 配置，例如端点、加密等

    // 定义 S3 基础路径
    val s3BasePath = "s3://emr-on-eks-nvme-776937568034-us-east-1/BLOG_TPCDS-TEST-100G-partitioned/"

    // 列出所有 TPC-DS 表
    val tables = Seq(
      "call_center",
      "catalog_page",
      "catalog_returns",
      "catalog_sales",
      "customer",
      "customer_address",
      "customer_demographics",
      "date_dim",
      "household_demographics",
      "income_band",
      "inventory",
      "item",
      "promotion",
      "reason",
      "ship_mode",
      "store",
      "store_returns",
      "store_sales",
      "time_dim",
      "warehouse",
      "web_page",
      "web_returns",
      "web_sales",
      "web_site"
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

    // 示例：读取 TPC-DS SQL 文件（假设 SQL 文件存储在 S3 或本地，可以根据需要调整）
    // 这里假设你有一个包含 TPC-DS 查询的 SQL 字符串
    val tpcdsSQL = """
      -- 示例查询：查询销售额最高的前 10 个商品
      SELECT
        i_item_id,
        i_item_desc,
        SUM(ss_sales_price) AS total_sales
      FROM
        catalog_sales
      JOIN
        item ON catalog_sales.ss_item_sk = item.i_item_sk
      GROUP BY
        i_item_id,
        i_item_desc
      ORDER BY
        total_sales DESC
      LIMIT 10
    """

    // 执行 SQL 查询
    println("Executing TPC-DS SQL Query...")
    val resultDF = spark.sql(tpcdsSQL)

    // 展示结果
    resultDF.show(truncate = false)

    // 你可以选择将结果保存回 S3 或其他存储
    // 例如，将结果保存为 Parquet 文件
    // resultDF.write.mode("overwrite").parquet("s3://your-output-bucket/path/to/results/")

    // 停止 SparkSession
    spark.stop()
  }
}

