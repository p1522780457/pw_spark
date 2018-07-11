package com.pangw.spark.streaming;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.pangw.util.JavaRedisClient;
import com.pangw.util.KafkaRedisConfig;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Tuple;
import scala.Tuple2;

import java.util.*;

public class UserClickCountAnalyza {
    public static void main(String[] args) throws Exception {
        SparkConf conf = new SparkConf().setAppName("userclickcountanalyze");
        conf.setMaster("local[2]");

        JavaStreamingContext ssc = new JavaStreamingContext(conf, Durations.seconds(5));

        String[] topics = KafkaRedisConfig.KAFKA_USER_TOPIC.split(" ");
        System.out.println("topic :" + Arrays.toString(topics));

        String brokers = KafkaRedisConfig.KAFKA_ADDR;
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list", brokers);
        kafkaParams.put("serializer.class", "kafka.serializer.StringEncoder");

        final String cllickHashKey = "app::users::click";

        JavaPairInputDStream<String, String> kafkaStream = KafkaUtils.createDirectStream(ssc,
                String.class, String.class,
                StringDecoder.class, StringDecoder.class,
                kafkaParams,
                new HashSet<String>(Arrays.asList(topics))
        );


        JavaDStream events = kafkaStream.map(new Function<Tuple2<String, String>, JSONObject>() {
            public JSONObject call(Tuple2<String, String> lines) throws Exception {
                System.out.println("kafkaUtils    -----        lines_1:" + lines._1() + "           lines_2:" + lines._2());
                JSONObject data = JSON.parseObject(lines._2());
                return data;
            }
        });

        JavaPairDStream<String, Long> userClicks = events.mapToPair(
                new PairFunction<JSONObject, String, Long>() {
                    public Tuple2<String, Long> call(JSONObject o) throws Exception {
                        return new Tuple2<String, Long>(o.getString("uid"), o.getLong("click_count"));
                    }
                }
        )
                .reduceByKey(new Function2<Long, Long, Long>() {
                    public Long call(Long s, Long s2) throws Exception {
                        return s + s2;
                    }
                });

        userClicks.foreachRDD(new VoidFunction<JavaPairRDD<String, Long>>() {
            public void call(JavaPairRDD<String, Long> rdd) throws Exception {
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String, Long>>>() {
                    public void call(Iterator<Tuple2<String, Long>> tuple2Iterator) throws Exception {
                        Jedis jedis = JavaRedisClient.get().getResource();
                        while (tuple2Iterator.hasNext()) {
                            Tuple2<String, Long> t = tuple2Iterator.next();
                            System.out.println("String :" + t._1() + "      Long :" + t._2());
                            jedis.hincrBy(cllickHashKey, t._1(), t._2());
                        }

                    }
                });
            }
        });

        ssc.start();
        ssc.awaitTermination();

    }
}
