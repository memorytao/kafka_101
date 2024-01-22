package com.kafka.app;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String topic = "prepay-sync-master";
        KafkaProducer<String, String> producer = ProducerConfigs.getKafkaProducer(null);

        for (int i = 0; i < 100; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, getDatta(topic));

            Headers headers = record.headers();
            headers.add("sync_date", "20240119".getBytes());
            headers.add("action", "update".getBytes());
            producer.send(record);
        }

        producer.flush();
        producer.close();
    }

    public static String getDatta(String topic) {

        String data = "";

        String currentPath = System.getProperty("user.dir");
        Path path = Paths.get(currentPath + "/app/src/main/java/com/kafka/app/" + topic + ".json");
        System.out.println(path.toAbsolutePath());

        try {

            JsonElement jsonElement = JsonParser.parseString(Files.readString(path));
            JsonArray jsonArray = jsonElement.getAsJsonArray();

            int index = (int) ((Math.random() * (3 - 0 + 1)) + 0);
            System.out.println(jsonArray.get(index).toString());
            return jsonArray.get(index).toString();

        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return data;

    }

}
