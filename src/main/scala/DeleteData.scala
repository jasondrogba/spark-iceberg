import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DeleteData {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession，并启用Hive支持
    val spark = SparkSession.builder()
      .appName("Delete First 500 Rows from Hive Table with Checkpoint")
      .config("spark.master", "local[*]")
      .config("spark.sql.warehouse.dir", "s3a://alluxio-tpch100/hive-test") // S3路径
      .config("hive.metastore.uris", "thrift://hive-metastore:9083") // Metastore URI
      .config("hive.metastore.warehouse.dir", "file:///opt/hive/warehouse")
      .config("spark.hadoop.fs.s3a.access.key", "xxx") // AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "xxx") // AWS Secret Key
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

    // 定义一个Window函数，按 'id' 列排序，并计算行号
    val windowSpec = Window.orderBy("id")

    // 为每一行生成行号，并过滤掉前500行
    val filteredDf = df.withColumn("row_num", row_number().over(windowSpec))
      .filter(col("row_num") > 500)
      .drop("row_num") // 删除临时列 'row_num'

    // 应用Checkpoint
    val checkpointedDf = filteredDf.checkpoint()

    // 覆盖写回Hive表
    checkpointedDf.write.mode("overwrite").saveAsTable(s"$dbName.$tableName")

    // 关闭SparkSession
    spark.stop()
  }
}
