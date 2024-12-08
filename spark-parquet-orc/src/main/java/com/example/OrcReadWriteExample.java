package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrcReadWriteExample {
    public static void main(String[] args) {
        // 创建一个SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("Java Spark ORC Read/Write Example")
                .master("local[*]")  // 在本地模式下运行
                .config("spark.sql.orc.impl", "native") // 使用原生ORC实现
                //.enableHiveSupport()                    // 如果你需要Hive支持
                .getOrCreate();

        try {
            // 指定输入和输出路径
            String inputPath = "file:///W:\\github\\spark-demo\\spark-parquet-orc\\src\\main\\resources\\data\\origin\\test.csv"; // 输入CSV文件路径
            String outputPath = "file:///W:\\github\\spark-demo\\spark-parquet-orc\\src\\main\\resources\\data\\orc"; // 输出ORC文件路径

            // 1. 写入ORC文件
            writeOrc(spark, inputPath, outputPath);

            // 2. 读取ORC文件
            readOrc(spark, outputPath);

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // 关闭SparkSession
            spark.stop();
        }
    }

    private static void writeOrc(SparkSession spark, String inputPath, String outputPath) {
        // 从CSV文件中读取数据到DataFrame
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true") // 如果CSV有标题行
                .option("inferSchema", "true") // 自动推断数据类型
                .load(inputPath);

        df.show();
        // 将DataFrame写入ORC文件
        df.write()
                .mode("overwrite") // 覆盖模式，其他模式如"append", "ignore", "error"
                .format("orc")
                .save(outputPath); // 输出路径

        System.err.println("Data written to ORC format successfully.");
    }

    private static void readOrc(SparkSession spark, String outputPath) {
        // 从ORC文件中读取数据到DataFrame
        Dataset<Row> df = spark.read()
                .format("orc")
                .load(outputPath); // ORC文件路径

        // 显示DataFrame的前20行
        df.show();

        // 打印DataFrame的schema
        df.printSchema();

        System.err.println("Data read from ORC format successfully.");
    }
}