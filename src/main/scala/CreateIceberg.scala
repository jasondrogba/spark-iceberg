import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CreateIceberg {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Glue Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000_v1"

    // 创建数据库
    spark.sql(s"CREATE DATABASE IF NOT EXISTS my_catalog.$dbName")

    // 创建表
    spark.sql(s"""
  CREATE TABLE my_catalog.$dbName.$tableName (
    id INT,
    name STRING,
    age INT
  )
  USING iceberg
  TBLPROPERTIES ('format-version'='1')
""")
    // 写入5000行数据到iceberg表
    val data = (1 to 5000).map(i => (i, s"Name_$i", i))
    val df = spark.createDataFrame(data).toDF("id", "name", "age")
    df.writeTo(s"my_catalog.$dbName.$tableName").append()

    spark.stop()
  }
}
