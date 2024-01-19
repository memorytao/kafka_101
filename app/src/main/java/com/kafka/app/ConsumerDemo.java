package com.kafka.app;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemo {

    public static Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        // Map<String,Object> props = ConsumerCogfigs.iniConsumer();
        KafkaConsumer<String, String> kafkaConsumer = ConsumerCogfigs.getKafkaConsumer(null);
        kafkaConsumer.subscribe(Arrays.asList("demo_java"));

        for (; 1 > 0;) {

            log.info("Polling");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));

            for (ConsumerRecord<String, String> record : records) {

                log.info("key: " + record.key());
                log.info("Partition: " + record.partition());
                log.info("Offset: " + record.offset());
                log.info("Value: "+ record.value());
            
            }
        }
    }
}
