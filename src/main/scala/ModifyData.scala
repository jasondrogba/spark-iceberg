import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
object ModifyData {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession，并启用Hive支持
    val spark = SparkSession.builder()
      .appName("Hive Table Example")
      .config("spark.master", "local[*]")
      .config("spark.sql.warehouse.dir", "s3a://alluxio-tpch100/hive-test") // S3路径
      .config("hive.metastore.uris", "thrift://hive-metastore:9083") // Metastore URI
      .config("spark.hadoop.fs.s3a.access.key", "AKIA3JZIWO4RHLFD7QAK") // AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "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") // AWS Secret Key
      .enableHiveSupport()
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000_2"
    val tableName = "mytable_5000_hive_2"

    val df = spark.table(s"$dbName.$tableName")

    // 修改前500行的name字段
    val updatedDf = df.withColumn("name",
      when(row_number().over(Window.orderBy("id")) <= 500, "updated_name")
        .otherwise(col("name"))
    )

    // 写回Hive表
    updatedDf.write.mode("overwrite").saveAsTable(s"$dbName.$tableName")

    // 关闭SparkSession
    spark.stop()
  }
}
