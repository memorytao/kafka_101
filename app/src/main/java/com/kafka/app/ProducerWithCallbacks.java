package com.kafka.app;

import java.util.Map;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerWithCallbacks {

    public static Logger log = LoggerFactory.getLogger(ProducerWithCallbacks.class.getSimpleName());

    public static void main(String[] args) {

        Map<String, Object> props = ProducerConfigs.initProducer();
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, "400");
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName());

        KafkaProducer<String, String> producer = ProducerConfigs.getKafkaProducer(props);

        for (int j = 0; j < 40; j++) {

            for (int i = 0; i < 30; i++) {

                ProducerRecord<String, String> record = new ProducerRecord<String, String>("demo_java", "hello" + i);
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        // TODO Auto-generated method stub
                        String details = " Received Topic:" + metadata.topic() + " Partition:" + metadata.partition()
                                + " Offset:" + metadata.offset();
                        log.info(details);
                    }
                });
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
            producer.flush();
            producer.close();
        }
    }

}
