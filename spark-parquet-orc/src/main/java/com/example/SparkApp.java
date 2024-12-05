package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import java.io.File;

public class SparkApp {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("Spark Parquet and ORC Example")
                .master("local[1]")
                .getOrCreate();

        String outputParquetPath = "/home/danil/ITMO_Projects/spark-parquet-orc/spark-parquet-orc/data/parquet";
        String outputOrcPath = "/home/danil/ITMO_Projects/spark-parquet-orc/spark-parquet-orc/data/orc";

        Dataset<Row> rawData = spark.read()
                .option("header", "true")
                .csv("/home/danil/ITMO_Projects/spark-parquet-orc/spark-parquet-orc/data/test_data.csv");

        Dataset<Row> transformedData = rawData.withColumn("totalTravelDistance*2", functions.col("totalTravelDistance").multiply(2));

        long parquetWriteStartTime = System.currentTimeMillis();

        transformedData.write()
                .mode("overwrite")
                .parquet(outputParquetPath);

        long parquetWtireEndTime = System.currentTimeMillis();
        long parquetWriteTime = parquetWtireEndTime - parquetWriteStartTime;

        System.out.println("Время записи Parquet: " + parquetWriteTime + " мс");
        
        
        long orcWriteStartTime = System.currentTimeMillis();

        transformedData.write()
                .mode("overwrite")
                .orc(outputOrcPath);

        long orcWriteEndTime = System.currentTimeMillis();
        long orcWriteTime = orcWriteEndTime - orcWriteStartTime;

        System.out.println("Время записи ORC: " + orcWriteTime + " мс");
        
        long parquetFileSize = getFileSize(outputParquetPath);
        long orcFileSize = getFileSize(outputOrcPath);

        System.out.println("Размер файла Parquet: " + parquetFileSize + " байт");
        System.out.println("Размер файла ORC: " + orcFileSize + " байт");

        long parquetReadStartTime = System.currentTimeMillis();

        Dataset<Row> parquetData = spark.read().parquet(outputParquetPath);
        parquetData.count();
        
        long parquetReadEndTime = System.currentTimeMillis();
        long parquetReadTime = parquetReadEndTime - parquetReadStartTime;

        System.out.println("Время чтения Parquet: " + parquetReadTime + " мс");


        long orcReadStartTime = System.currentTimeMillis();

        Dataset<Row> orcData = spark.read().orc(outputOrcPath);
        orcData.count();

        long orcReadEndTime = System.currentTimeMillis();
        long orcReadTime = orcReadEndTime - orcReadStartTime;

        System.out.println("Время чтения ORC: " + orcReadTime + " мс");

        spark.stop();
    }

    private static long getFileSize(String path) {
        File file = new File(path);
        long length = 0;
        if (file.isDirectory()) {
            for (File f : file.listFiles()) {
                length += f.length();
            }
        } else {
            length = file.length();
        }
        return length;
    }
}