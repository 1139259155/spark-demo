package com.example;

// 导入必要的 Spark 类

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Temp1 {
    public static void main(String[] args) throws SQLException {

        // 创建 Spark 会话
        SparkSession spark = SparkSession.builder()
                .appName("Spark Read HDFS CSV and Write to SQLite")
                .master("local[*]") // 在本地所有可用核心上运行
                .getOrCreate();

        // 读取 HDFS 上的 CSV 文件
        String hdfsPath = "hdfs://your-hdfs-namenode:8020/path/to/your/data.csv"
        Dataset<Row> df = spark.read()
                .option("header", "true") // 如果 CSV 文件有表头
                .option("inferSchema", "true") // 自动推断 schema
                .csv(hdfsPath);

// 显示原始数据
        df.show();

        // 使用 Groovy 脚本处理数据
// 示例：过滤出 age > 20 的记录
        //Dataset<Row> processedDf = df.filter(df.col("age") > 20);

// 显示处理后的数据
        df.show();

        // 准备将数据写入 SQLite 数据库
        String jdbcUrl = "jdbc:sqlite:/path/to/your/database.db";
        String tableName = "processed_data";

// 注册 DataFrame 为临时视图
        df.createOrReplaceTempView("temp_view");

        // 执行 SQL 查询并将结果写入 SQLite
        Connection connection = DriverManager.getConnection(jdbcUrl);
        try {
            // 创建表（如果不存在）
            String createTableSql = " CREATE TABLE IF NOT EXISTS $tableName ( name TEXT, age INTEGER) ";
            connection.createStatement().execute(createTableSql);

            // 插入数据
            String insertSql = "INSERT INTO $tableName (name, age) VALUES (?, ?)";
            PreparedStatement pstmt = connection.prepareStatement(insertSql);

            df.collect().each {
                row ->
                        pstmt.setString(1, row.getAs("name"));
                pstmt.setInt(2, row.getAs("age"));
                pstmt.executeUpdate();
            }

            //  println "Data written to SQLite database successfully.";
        } finally {
            connection.close();
        }

        // 停止 Spark 会话
        spark.stop();


    }
}


