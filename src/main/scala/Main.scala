import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object IcebergGlueExample {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Glue Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb4"
    val tableName = "mytable4"

//    // 写入数据到Iceberg表
//    val data = Seq((1, "Alice", 66), (2, "Bob", 31), (3, "Charlie", 25))
//    val df = spark.createDataFrame(data).toDF("id", "name", "age")
//
//    df.writeTo(s"my_catalog.$dbName.$tableName").createOrReplace()
    println("Data read from Iceberg table!")
    // 从Iceberg表中读取数据
    val icebergDF = spark.read.format("iceberg").load(s"my_catalog.$dbName.$tableName")
    icebergDF.show()
    // 停止SparkSession
    spark.stop()
  }
}
