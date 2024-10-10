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
      .config("hive.metastore.warehouse.dir", "file:///opt/hive/warehouse")
      .config("spark.hadoop.fs.s3a.access.key", "AKIA3JZIWO4RHLFD7QAK") // AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "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") // AWS Secret Key
      .enableHiveSupport()
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000_3"
    val tableName = "mytable_5000_hive_3"

    // 使用数据库
    spark.sql(s"USE $dbName")

    // 设置Checkpoint目录
    val checkpointDir = "s3a://alluxio-tpch100/hive-test/checkpoints"
    spark.sparkContext.setCheckpointDir(checkpointDir)

    // 读取表数据
    var df = spark.table(s"$dbName.$tableName")

    // 应用Checkpoint
    df = df.checkpoint()

    // 修改前500行的name字段
    val windowSpec = Window.orderBy("id")
    val updatedDf = df.withColumn("name",
      when(row_number().over(windowSpec) <= 500, "consistent")
        .otherwise(col("name"))
    )

    // 写回Hive表，使用 overwrite 模式
    updatedDf.write.mode("overwrite").saveAsTable(s"$dbName.$tableName")

    // 关闭SparkSession
    spark.stop()
  }
}
