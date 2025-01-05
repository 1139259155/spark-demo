package com.example;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.*;

public class LocalCSVToHDFSSparkStreaming {

    public static void main(String[] args) throws Exception {
        // 设置Spark配置
        SparkConf conf = new SparkConf().setAppName("LocalCSVToHDFSSparkStreaming").setMaster("local[2]");
        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.minutes(10));

        // 模拟从本地文件系统读取CSV文件流
        // 注意：这里假设有一个机制可以定期将新的CSV文件放入指定目录
        String inputDir = "file:///W:\\github\\spark-demo\\spark-parquet-orc\\src\\main\\resources\\data\\origin\\test.csv"; // 替换为实际的本地文件路径
        // JavaReceiverInputDStream<String> lines = ssc.textFileStream(inputDir);
        JavaDStream<String> lines = ssc.textFileStream(inputDir);

        // 处理数据（例如，简单地过滤掉空行）
        JavaDStream<String> nonEmptyLines = lines.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String line) throws Exception {
                return !line.isEmpty();
            }
        });

        // 每10分钟批量写入HDFS
        nonEmptyLines.foreachRDD(rdd -> {
            if (!rdd.isEmpty()) {
                try {
                    // 获取当前时间戳作为输出目录的一部分
                    long timestamp = System.currentTimeMillis();
                    String outputDir = "hdfs://namenode:8020/user/youruser/output/timestamp=" + timestamp;

                    // 将RDD保存为文本文件到HDFS
                    JavaRDD<String> filteredRDD = rdd.coalesce(1); // 合并为单个分区以简化文件管理
                    filteredRDD.saveAsTextFile(outputDir);

                    // 可选：清理旧文件或进行其他后处理操作
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // 启动流处理
        ssc.start();
        ssc.awaitTermination();
    }
}