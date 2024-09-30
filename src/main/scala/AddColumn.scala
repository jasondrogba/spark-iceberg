import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object AddColumn {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession，并启用Hive支持
    val spark = SparkSession.builder()
      .appName("Add Column to Hive Table with Checkpoint")
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

    // 设置Checkpoint目录
    val checkpointDir = "s3a://alluxio-tpch100/hive-test/checkpoints"
    spark.sparkContext.setCheckpointDir(checkpointDir)

    // 读取表数据
    var df = spark.table(s"$dbName.$tableName")

    // 应用Checkpoint
    df = df.checkpoint()

    // 添加新列 'new_column'，这里以默认值 'default_value' 为例
    val updatedDf = df.withColumn("new_column", lit("default_value"))

    // 或者，如果新列基于现有列进行计算，例如基于 'id' 列的值
    // val updatedDf = df.withColumn("new_column", col("id") * 2)

    // 覆盖写回Hive表
    updatedDf.write.mode("overwrite").saveAsTable(s"$dbName.$tableName")

    // 关闭SparkSession
    spark.stop()
  }
}
