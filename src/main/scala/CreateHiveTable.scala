import org.apache.spark.sql.SparkSession

object CreateHiveTable {
  def main(args: Array[String]): Unit = {
    // 创建SparkSession，并启用Hive支持
    val spark = SparkSession.builder()
      .appName("Hive Table Example")
      .config("spark.master", "local[*]")
//      .config("spark.sql.warehouse.dir", "s3://your-bucket/path") // S3路径
//      .config("hive.metastore.uris", "thrift://your-metastore-host:port") // Metastore URI
      .enableHiveSupport()
      .getOrCreate()

    // 定义数据库和表名
    val dbName = "mydb_5000"
    val tableName = "mytable_5000_hive"

    // 创建数据库
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")

    // 创建数据库
    spark.sql(s"CREATE DATABASE IF NOT EXISTS $dbName")
    spark.sql(s"USE $dbName")

    // 创建Hive表
    spark.sql(s"""
      CREATE TABLE IF NOT EXISTS $dbName.$tableName (
        id INT,
        name STRING,
        age INT
      )
      STORED AS PARQUET
      LOCATION 's3a://alluxio-tpch100/hive-test/$dbName/$tableName'
    """)

    // 写入5000行数据到Hive表
    val data = (1 to 5000).map(i => (i, s"Name_$i", i))
    val df = spark.createDataFrame(data).toDF("id", "name", "age")

    // 写入数据到Hive表
    df.write.mode("append").insertInto(s"$dbName.$tableName")

    spark.stop()
  }
}
