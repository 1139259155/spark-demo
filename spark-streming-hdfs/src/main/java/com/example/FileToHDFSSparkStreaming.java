package com.example;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import scala.Tuple2;

import java.net.URI;
import java.util.Arrays;

public class FileToHDFSSparkStreaming {

    public static void main(String[] args) throws Exception {
        // 创建SparkConf并配置为本地模式
        SparkConf conf = new SparkConf().setAppName("FileToHDFSSparkStreaming").setMaster("local[2]");

        // 创建JavaStreamingContext，批处理间隔为10分钟
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.minutes(10));

        // 监控本地目录，每当有新文件时触发处理
        String inputDir = "file:///path/to/local/input/directory"; // 替换为你的本地目录路径
        /*jssc.fileStream(inputDir, String.class, LongWritable.class, TextInputFormat.class)
                .foreachRDD(rdd -> {
                    if (!rdd.isEmpty()) {
                        // 处理逻辑：例如，统计单词数量
                        JavaPairRDD<String, Integer> wordCounts = rdd.flatMap(line -> Arrays.asList(line.split(" ")).iterator())
                                .mapToPair(word -> new Tuple2<>(word, 1))
                                .reduceByKey((a, b) -> a + b);

                        // 将结果写入HDFS
                        String outputDir = "hdfs://namenode:8020/user/youruser/output";
                        writeWordCountsToHDFS(wordCounts, outputDir);
                    }
                });*/

        // 启动流处理上下文
        jssc.start();
        jssc.awaitTermination();
    }

    private static void writeWordCountsToHDFS(JavaPairRDD<String, Integer> wordCounts, String outputDir) {
        try {
            // 获取HDFS的FileSystem实例
            Configuration hadoopConf = new Configuration();
            FileSystem fs = FileSystem.get(new URI(outputDir), hadoopConf);

            // 如果输出目录存在，则删除以避免覆盖
            Path outputPath = new Path(outputDir);
            if (fs.exists(outputPath)) {
                fs.delete(outputPath, true);
            }

            // 将结果保存到HDFS
            wordCounts.saveAsTextFile(outputDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}