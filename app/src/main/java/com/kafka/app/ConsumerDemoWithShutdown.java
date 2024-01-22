package com.kafka.app;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

public class ConsumerDemoWithShutdown {

    public static Logger log = new LogConfig(ConsumerDemoWithShutdown.class.getSimpleName()).getLogger();

    public static void main(String[] args) {

        log.info("Hi");

        KafkaConsumer<String, String> consumer = ConsumerCogfigs.getKafkaConsumer(null);

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
            
            long endTime =  System.currentTimeMillis()+20000;

            consumer.subscribe(Arrays.asList("wikimedia.recentchange"));
            for (; System.currentTimeMillis() < endTime ;) {

                log.info("Polling");

                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

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
        }finally{

            consumer.close();
            log.info(" Consumer fully shutdown");
        }

    }
}
