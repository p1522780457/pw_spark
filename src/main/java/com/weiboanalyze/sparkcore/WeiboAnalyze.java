package com.weiboanalyze.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class WeiboAnalyze {
    public static String path_follower = "data/weibo/follower_followee.csv";
    public static String path_user = "/Users/pangw/pw_workspace/IdeaProjects/pw_spark/data/weibo/weibo_user.csv";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("weiboAnalyze")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> linesRdd = sc.textFile(path_follower);

        JavaPairRDD<String, String> user = sc.textFile(path_user)
                .mapToPair(new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        return new Tuple2<String, String>(lines[0], lines[1]);
                    }
                });


        final String first = linesRdd.first();


        JavaPairRDD<String, String> id_followString = linesRdd
                .filter(new Function<String, Boolean>() {
                    public Boolean call(String s) throws Exception {
                        return s != first;
                    }
                })
                .mapToPair(new PairFunction<String, String, String>() {
                    public Tuple2<String, String> call(String s) throws Exception {
                        String[] lines = s.split(",");
                        return new Tuple2<String, String>(lines[0], lines[1] + "," + lines[2] + "," + lines[3] + "," + lines[4]);
                    }
                })
                .join(user)
                .map(new Function<Tuple2<String, Tuple2<String, String>>, Tuple2<String, String>>() {
                    public Tuple2<String, String> call(Tuple2<String, Tuple2<String, String>> stringTuple2Tuple2) throws Exception {
                        String key = stringTuple2Tuple2._1();
                        String value = stringTuple2Tuple2._2()._1() + "," + stringTuple2Tuple2._2()._2();
                        return new Tuple2<String, String>(key, value);
                    }
                })
                /**
                 *
                 * id   :  follow , followid , followee , followeeid , usename
                 *
                 */
                .flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                    public Iterator<Tuple2<String, String>> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        String[] lines = stringStringTuple2._2().split(",");
                        String id = stringStringTuple2._1();
                        String follower = lines[0];
                        String followet_id = lines[1];
                        String followee = lines[2];
                        String followee_id = lines[3];
                        String id_name = lines[4];

                        List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();

                        list.add(stringStringTuple2);
                        Tuple2<String, String> newT = new Tuple2<String, String>(followet_id, " , ," + id_name + "," + id + "," + followee);
                        list.add(newT);

                        return list.iterator();
                    }
                })
                .mapToPair(new PairFunction<Tuple2<String, String>, String, String>() {
                    public Tuple2<String, String> call(Tuple2<String, String> stringStringTuple2) throws Exception {
                        String[] lines = stringStringTuple2._2().split(",");
                        String id = stringStringTuple2._1();
                        String follower = lines[0];
                        String followet_id = lines[1];
                        String followee = lines[2];
                        String followee_id = lines[3];
                        String id_name = lines[4];
                        return new Tuple2<String, String>(id, followee);
                    }
                })
                .reduceByKey(new Function2<String, String, String>() {
                    public String call(String s, String s2) throws Exception {
                        return s + "+" + s2;
                    }
                });

        id_followString.foreach(new VoidFunction<Tuple2<String, String>>() {
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2._1() + "-->" + stringStringTuple2._2());
            }
        });


//
//        //过滤掉首行
//        JavaPairRDD<String, String> id_followee = linesRdd.filter(new Function<String, Boolean>() {
//            public Boolean call(String s) throws Exception {
//                return s != first;
//            }
//        }).flatMap(new FlatMapFunction<String, String>() {
//            public Iterator<String> call(String s) throws Exception {
//                String[] lines = s.split(",");
//                String id = lines[0];
//                String follower = lines[1];
//                String followet_id = lines[2];
//                String followee = lines[3];
//                String followee_id = lines[4];
//                List<String> list = new ArrayList<String>();
//                list.add(s);
//                String newS = followet_id + ", , , ," + id;
//                list.add(newS);
//                return list.iterator();
//            }
//        }).mapToPair(new PairFunction<String, String, String>() {
//            public Tuple2<String, String> call(String s) throws Exception {
//                String[] lines = s.split(",");
//                return new Tuple2<String, String>(lines[0], lines[3]);
//            }
//        }).reduceByKey(new Function2<String, String, String>() {
//            public String call(String s, String s2) throws Exception {
//
//                return s + "::" + s2;
//            }
//        });
//        id_followee.foreach(new VoidFunction<Tuple2<String, String>>() {
//            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
//                System.out.println(stringStringTuple2._1() + "-->" + stringStringTuple2._2());
//            }
//        });


    }
}
