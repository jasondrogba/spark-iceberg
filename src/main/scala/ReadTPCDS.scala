import org.apache.spark.sql.SparkSession

object ReadTPCDS {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Hive Example")
      .enableHiveSupport()
      .config("spark.hadoop.fs.s3a.access.key", "xxx") // AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "xxx") // AWS Secret Key
      .getOrCreate()

    // 定义 S3 基础路径
    val s3BasePath = "s3a://emr-on-eks-nvme-776937568034-us-east-1/BLOG_TPCDS-TEST-100G-partitioned/"

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

    val tpcdsSQL ="""
 select i_brand_id brand_id, i_brand brand, i_manufact_id, i_manufact,
 	sum(ss_ext_sales_price) ext_price
 from date_dim, store_sales, item,customer,customer_address,store
 where d_date_sk = ss_sold_date_sk
   and ss_item_sk = i_item_sk
   and i_manager_id = 8
   and d_moy = 11
   and d_year = 1998
   and ss_customer_sk = c_customer_sk
   and c_current_addr_sk = ca_address_sk
   and substr(ca_zip,1,5) <> substr(s_zip,1,5)
   and ss_store_sk = s_store_sk
 group by i_brand, i_brand_id, i_manufact_id, i_manufact
 order by ext_price desc, brand, brand_id, i_manufact_id, i_manufact
 limit 100
    """




    // 执行 SQL 查询并测量执行时间
    println("Executing TPC-DS SQL Query...")
    val startTime = System.nanoTime()

    val resultDF = spark.sql(tpcdsSQL)

    // 触发动作以确保查询执行完成
    resultDF.collect()

    val endTime = System.nanoTime()
    val durationSeconds = (endTime - startTime) / 1e9

    println(f"SQL Query Execution Time: $durationSeconds%.3f seconds")

    // 展示结果
    resultDF.show(truncate = false)

    // 你可以选择将结果保存回 S3 或其他存储
    // 例如，将结果保存为 Parquet 文件
    // resultDF.write.mode("overwrite").parquet("s3://your-output-bucket/path/to/results/")

    // 停止 SparkSession
    spark.stop()
  }
}
