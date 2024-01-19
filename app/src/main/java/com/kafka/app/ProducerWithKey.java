package com.kafka.app;

import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithKey {

    public static Logger log = LoggerFactory.getLogger(ProducerWithKey.class.getSimpleName());

    public static void main(String[] args) {

        Map<String, Object> props = ProducerConfigs.initProducer();
        KafkaProducer<String, String> producer = ProducerConfigs.getKafkaProducer(props);

        for (int i = 0; i < 40; i++) {

            String topic = "demo_java";
            String key = "id_" + i;
            String value = "hello " + i;

            ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);

            producer.send(record, new Callback() {

                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // TODO Auto-generated method stub
                    String details = " Received Topic:" + metadata.topic() + " Key:" + key + " Partition:"
                            + metadata.partition()
                            + " Offset:" + metadata.offset() + " Value:" + value;
                    log.info(details);
                }
            });

        }
        producer.flush();
        producer.close();
    }
}
