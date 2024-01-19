package com.kafka.app;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

public class ProducerConfigs {

    public static Map<String, Object> initProducer() {

        Map<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    public static KafkaProducer<String, String> getKafkaProducer(Map<String, Object> producer) {

        if (producer == null)
            producer = initProducer();
        return new KafkaProducer<>(producer);
    }
}
