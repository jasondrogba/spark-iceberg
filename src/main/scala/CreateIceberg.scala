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
    val tableName = "mytable_5000"

    // 写入5000行数据到iceberg表
    val data = (1 to 5000).map(i => (i, s"Name_$i", i))
    val df = spark.createDataFrame(data).toDF("id", "name", "age")
    df.writeTo(s"my_catalog.$dbName.$tableName").createOrReplace()

    spark.stop()
  }
}
