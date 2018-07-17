package com.sogoanalyze.sparkcore;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * 统计每分钟的pv(访问总量) 和 uv（独立用户访问数） 输出形式为 时间 pv uv
 * spark rdd 实现
 */
public class AnalyzeSpark01 {
    public static String path_sogo = "data/sogoanalyze/SogouQ.sample";

    public static void main(String[] args) {
        final SparkConf conf = new SparkConf().setAppName("sogoAnalyza")
                .setMaster("local[1]");

        JavaSparkContext sc = new JavaSparkContext(conf);

        final JavaRDD<String> lineRdd = sc.textFile(path_sogo);
        //筛选 id  pv

        JavaPairRDD<String, Integer> pv = lineRdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split("  ");
                String newKey = lines[0].substring(0, 5);
                return new Tuple2<String, Integer>(newKey, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        JavaPairRDD<String, Integer> uv = lineRdd.mapToPair(new PairFunction<String, String, Integer>() {
            public Tuple2<String, Integer> call(String s) throws Exception {
                String[] lines = s.split("\t");
                String newKey = "";
                if (lines.length == 5) {
                    newKey = lines[0].substring(0, 5) + "," + lines[1];
                }
                return new Tuple2<String, Integer>(newKey, 1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return 1;
            }
        }).mapToPair(new PairFunction<Tuple2<String, Integer>, String, Integer>() {
            public Tuple2<String, Integer> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                String newKey = stringIntegerTuple2._1().split(",")[0];
                return new Tuple2<String, Integer>(newKey, stringIntegerTuple2._2());
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });
        pv.join(uv).foreach(new VoidFunction<Tuple2<String, Tuple2<Integer, Integer>>>() {
            public void call(Tuple2<String, Tuple2<Integer, Integer>> stringTuple2Tuple2) throws Exception {
                System.out.println(stringTuple2Tuple2._1() + "," + stringTuple2Tuple2._2()._1() + "," + stringTuple2Tuple2._2()._2());
            }
        });
        /**
         * 结果输出
         00:06,1036,818
         00:04,1056,830
         00:08,1024,783
         00:05,1024,809
         00:07,999,785
         00:00,1046,827
         00:01,1046,797
         00:09,630,535
         00:02,1088,835
         00:03,1051,835
         */

    }


}
