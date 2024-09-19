import org.apache.spark.sql.SparkSession

object AddColumn {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Add Column Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000"

    // 使用 ALTER TABLE 语句添加新列
    spark.sql(
      s"""
         |ALTER TABLE my_catalog.$dbName.$tableName
         |ADD COLUMN new_column STRING
         |""".stripMargin
    )

    // 获取表的schema，并统计列数
    val schema = spark.read.format("iceberg")
      .load(s"my_catalog.$dbName.$tableName")
      .schema

    // 输出表的总列数
    println(s"Total number of columns: ${schema.fields.length}")
  }
}
