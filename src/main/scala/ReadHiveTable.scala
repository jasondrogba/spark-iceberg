import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadHiveTable {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Hive Example")
      .enableHiveSupport()
      .config("spark.sql.warehouse.dir", "s3a://alluxio-tpch100/hive-test") // S3路径
      .config("hive.metastore.uris", "thrift://hive-metastore:9083") // Metastore URI
      .config("spark.hadoop.fs.s3a.access.key", "xxx") // AWS Access Key
      .config("spark.hadoop.fs.s3a.secret.key", "xxx") // AWS Secret Key
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000_2"
    val tableName = "mytable_5000_hive_2"

    // 创建数据库
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    spark.sql(s"USE $dbName")

    // 统计表的行数
    val rowCount = spark.sql(s"SELECT COUNT(*) FROM $dbName.$tableName").first().getLong(0)
    println(s"总行数: $rowCount")

    // 读取前500行
    val df = spark.sql(s"SELECT * FROM $dbName.$tableName LIMIT 500")

    // 打印20行数据
    df.show(20)  // 停止SparkSession
    spark.stop()
  }
}
