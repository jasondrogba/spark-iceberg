import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions

object ModifyData {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession
    val spark = SparkSession.builder()
      .appName("Iceberg Update Example")
      .config("spark.master", "local[*]")
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000_v1"

    println("Data modified from Iceberg table!")

    // 使用 MERGE INTO 进行更新操作
    spark.sql(
      s"""
         |MERGE INTO my_catalog.$dbName.$tableName AS target
         |USING (
         |  SELECT id, 'Updated_Name' AS name
         |  FROM my_catalog.$dbName.$tableName
         |  WHERE id BETWEEN 1 AND 500
         |) AS source
         |ON target.id = source.id
         |WHEN MATCHED THEN
         |  UPDATE SET target.name = source.name
         |""".stripMargin
    )
  }
}
