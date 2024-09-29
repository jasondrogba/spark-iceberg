import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ReadIceberg {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Glue Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000_v1"

    println("Data read from Iceberg table!")
    // 从Iceberg表中读取数据
    val icebergDF = spark.read.format("iceberg").load(s"my_catalog.$dbName.$tableName")
    // 统计行数
    val rowCount = icebergDF.count()
    println(s"Total number of rows in the table: $rowCount")

    // 获取列数
    val columnCount = icebergDF.columns.length
    println(s"Total number of columns in the table: $columnCount")

    // 打印前几行数据以验证
    icebergDF.show(20)    // 停止SparkSession
    spark.stop()
  }
}
