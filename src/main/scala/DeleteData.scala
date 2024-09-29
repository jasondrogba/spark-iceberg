import org.apache.spark.sql.SparkSession

object DeleteData {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Delete Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000_v1"

    // 删除满足条件的记录 (id 在 1 到 500 之间的记录)
    spark.sql(
      s"""
         |DELETE FROM my_catalog.$dbName.$tableName
         |WHERE id BETWEEN 1 AND 500
         |""".stripMargin
    )

    // 统计删除后表中的总行数
    val rowCount = spark.sql(s"SELECT COUNT(*) AS total FROM my_catalog.$dbName.$tableName")
      .collect()(0).getAs[Long]("total")

    // 打印删除后的总行数
    println(s"Total rows after deletion: $rowCount")
  }
}
