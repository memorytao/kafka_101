package com.kafka.app;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

public class ConsumerDemoCooperative {

    public static Logger log = new LogConfig(ConsumerDemoCooperative.class.getSimpleName()).getLogger();

    public static void main(String[] args) {

        log.info("Hi");

        Map<String, Object> configs = new ConsumerCogfigs().iniConsumer();
        configs.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName());
        KafkaConsumer<String, String> consumer = ConsumerCogfigs.getKafkaConsumer(configs);

        final Thread thread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // TODO Auto-generated method stub
                // super.run();
                log.info("Detected shutdown..............");
                consumer.wakeup();

                try {
                    thread.join();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        });

        try {

            consumer.subscribe(Arrays.asList("demo_java"));
            for (; 1 > 0;) {

                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

                for (ConsumerRecord<String, String> record : records) {

                    log.info("key: " + record.key());
                    log.info("Partition: " + record.partition());
                    log.info("Offset: " + record.offset());
                    log.info("Value: " + record.value());

                }
            }
        } catch (WakeupException wakeupException) {
            log.info("Consumer is starting shut down");

        } catch (Exception e) {
            // TODO: handle exception
            log.error(" Unexpected ");
        } finally {

            consumer.close();
            log.info(" Consumer fully shutdown");
        }

    }
}
