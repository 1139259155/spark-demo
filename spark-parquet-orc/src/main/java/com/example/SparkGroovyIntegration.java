package com.example;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.codehaus.groovy.control.CompilerConfiguration;
import groovy.lang.GroovyShell;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SparkGroovyIntegration {

    public static void main(String[] args) throws Exception {
        // 初始化SparkSession
        SparkSession spark = SparkSession.builder()
                .appName("SparkGroovyIntegration")
                .config("spark.master", "local") // 设置为你的集群配置
                .getOrCreate();

        // 读取HDFS上的CSV文件
        String csvFilePath = "hdfs://path/to/your/file.csv";
        Dataset<Row> df = spark.read().option("header", "true").csv(csvFilePath);

        // 初始化Groovy Shell
        CompilerConfiguration conf = new CompilerConfiguration();
        GroovyShell shell = new GroovyShell(conf);

        // 加载Groovy脚本
        String scriptText = ""
                + "class ProcessData {\n"
                + "    static def processDataFrame(df) {\n"
                + "        // 在这里添加你的Groovy代码来处理DataFrame\n"
                + "        return df.withColumn('new_column', lit('processed'))\n"
                + "    }\n"
                + "}\n";

        // 执行Groovy脚本
        shell.evaluate(scriptText);

        // 获取Groovy类
        Class<?> processDataClass = shell.getClassLoader().loadClass("ProcessData");

        // 调用Groovy方法处理DataFrame
        Dataset<Row> processedDf = (Dataset<Row>) processDataClass.getMethod("processDataFrame", org.apache.spark.sql.Dataset.class)
                .invoke(null, df);

        // 显示结果
        processedDf.show();

        // 将处理后的DataFrame写入SQLite数据库
        writeDataFrameToSQLite(processedDf, "jdbc:sqlite:/path/to/output.db", "output_table");

        // 停止Spark Session
        spark.stop();
    }

    private static void writeDataFrameToSQLite(Dataset<Row> df, String url, String tableName) throws Exception {
        Connection connection = DriverManager.getConnection(url);
        String createTableSQL = "CREATE TABLE IF NOT EXISTS " + tableName + " (id INTEGER PRIMARY KEY AUTOINCREMENT, data TEXT)";
        try (PreparedStatement stmt = connection.prepareStatement(createTableSQL)) {
            stmt.executeUpdate();
        }

        // 写入数据
        for (Row row : df.collectAsList()) {
            StringBuilder columns = new StringBuilder();
            StringBuilder values = new StringBuilder();
            for (int i = 0; i < row.size(); i++) {
                if (i > 0) {
                    columns.append(", ");
                    values.append(", ");
                }
                columns.append(row.schema().fields()[i].name());
                values.append("'").append(row.get(i)).append("'");
            }
            String insertSQL = "INSERT INTO " + tableName + " (" + columns.toString() + ") VALUES (" + values.toString() + ")";
            try (PreparedStatement stmt = connection.prepareStatement(insertSQL)) {
                stmt.executeUpdate();
            }
        }

        connection.close();
    }
}