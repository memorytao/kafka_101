package com.kafka.app;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public class ProducerDemo {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) {

        String topic = "prepay-sync-master";
        KafkaProducer<String, String> producer = ProducerConfigs.getKafkaProducer(null);

        for (int i = 0; i < 100; i++) {

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, getDatta());
            
            Headers headers = record.headers();
            headers.add("sync_date", "20240119".getBytes());
            headers.add("action", "update".getBytes());
            producer.send(record);
            producer.flush();
        }
        producer.close();
    }

    public static String getDatta() {

        String data = "";

        String json = """
                {
                    "createdBy": "admin",
                    "createdDate": "2023-12-28 04:02:00.000",
                    "updatedBy": "admin",
                    "updatedDate": "2023-12-28 04:02:00.000",
                    "id": 6152,
                    "packageBuyCode": "TEST_BUY_PACK_2312281725",
                    "packageAddCode": "TEST_ADD_PACK_2312281725",
                    "bundleName": "Big_Bonus5",
                    "packageType": "DATA",
                    "chargeType": "OC",
                    "bundleType": "MAIN",
                    "bundleAmount": 0.0,
                    "bundleValidity": "1",
                    "bundleValidityUnit": "D",
                    "bundleGroupDisplay": 0,
                    "ccpBundle": 0,
                    "packageSpeed": "100",
                    "priceVatExc": 100.0,
                    "priceVatInt": 120.0,
                    "pricePriority": 1,
                    "endpointBuy": "SBM3G",
                    "endpointCancel": "SBM3G",
                    "effectiveDate": "2024-01-03 02:04:58.000",
                    "loyaltyFlag": 0,
                    "syncLoyalty": 1
                  }
                    """;

        JsonElement jsonElement = JsonParser.parseString(json);
        data = jsonElement.getAsJsonObject().toString();
        return data;

    }

}
