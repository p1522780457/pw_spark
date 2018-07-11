package com.pangw.kafka;

import com.alibaba.fastjson.JSONObject;
import com.pangw.util.KafkaRedisConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;

public class JavaKafkaProducer {

    private static String[] users = {
            "4A4D769EB9679C054DE81B973ED5D768", "8dfeb5aaafc027d89349ac9a20b3930f",
            "011BBF43B89BFBF266C865DF0397AA71", "f2a8474bf7bd94f0aabbd4cdd2c06dcf",
            "068b746ed4620d25e26055a9f804385f", "97edfc08311c70143401745a03a50706",
            "d7f141563005d1b5d0d3dd30138f3f62", "c8ee90aade1671a21336c721512b817a",
            "6b67c8c700427dee7552f81f3228c927", "a95f22eabc4fd4b580c011a3161a9d9d"};
    private static Random random = new Random();

    private static int pointer = -1;

    private static String getUserID() {
        pointer = pointer + 1;
        if(pointer >= users.length) {
            pointer = 0;
            return users[pointer];
        } else {
            return users[pointer];
        }
    }
    private static double click() {
        return random.nextInt(10);
    }

    public static void main(String[] args) throws Exception {
        String topic = KafkaRedisConfig.KAFKA_USER_TOPIC;
        String brokers = KafkaRedisConfig.KAFKA_ADDR;

        Map<String,String> props = new HashMap<String, String>();
        props.put("bootstrap.servers", brokers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer producer = new KafkaProducer(props);
         while (true){
             JSONObject event = new JSONObject();
             event.put("uid", getUserID());
             event.put("event_time", System.currentTimeMillis());
             event.put("os_type", "Android");
             event.put("click_count", click());
             producer.send(new ProducerRecord(topic,event.toString()));
             Thread.sleep(1000);
         }


    }
}
