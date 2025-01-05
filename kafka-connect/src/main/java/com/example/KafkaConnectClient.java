package com.example;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class KafkaConnectClient {

    private static final String KAFKA_CONNECT_URL = "http://kafka-connect:8083/connectors";

    public static void createConnector(String configFilePath) throws Exception {
        URL url = new URL(KAFKA_CONNECT_URL);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod("POST");
        connection.setRequestProperty("Content-Type", "application/json");
        connection.setDoOutput(true);

        // 读取配置文件内容
        String configJson = new String(Files.readAllBytes(Paths.get(configFilePath)));

        try (OutputStream os = connection.getOutputStream()) {
            byte[] input = configJson.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        int responseCode = connection.getResponseCode();
        if (responseCode == HttpURLConnection.HTTP_CREATED) {
            System.out.println("Connector created successfully.");
        } else {
            System.err.println("Failed to create connector. Response code: " + responseCode);
        }
    }

    public static void main(String[] args) {
        try {
            createConnector("hdfs-sink.json");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}