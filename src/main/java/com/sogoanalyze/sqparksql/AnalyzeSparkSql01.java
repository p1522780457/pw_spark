package com.sogoanalyze.sqparksql;

import com.sogoanalyze.model.SogoBean;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * 统计每分钟的pv(访问总量) 和 uv（独立用户访问数） 输出形式为 时间 pv uv
 * spark sql 实现
 */
public class AnalyzeSparkSql01 {
    public static String path_sogo = "data/sogoanalyze/SogouQ.sample";

    public static void main(String[] args) {
        /**
         * 创建
         * 注册成临时表
         * 通过sql求解
         */
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("sogoAnalyze")
                .getOrCreate();

        JavaRDD<SogoBean> useRdd = spark.read()
                .textFile(path_sogo)
                .toJavaRDD()
                .map(new Function<String, SogoBean>() {
                    public SogoBean call(String s) throws Exception {
                        String[] lines = s.split("\t");
                        SogoBean each = new SogoBean();
                        try {
                            each.setTime(lines[0].substring(0, 5));
                            each.setUid(lines[1]);
                            each.setContent(lines[2]);
                            each.setCount_order(lines[3]);
                            each.setCount_click(lines[4]);
                            each.setUrl(lines[5]);
                        } catch (Exception e) {
                        }
                        return each;
                    }
                });
        Dataset<Row> sogoDF = spark.createDataFrame(useRdd, SogoBean.class);

        sogoDF.createOrReplaceTempView("sogo");

        sogoDF.printSchema();

//        Dataset<Row> fins = spark.sql("select time, count(*) from sogo group by time");


        Dataset<Row> fins = spark.sql("select time, count(*), count(distinct uid) from sogo group by time");

        fins.show();


        /**
         +-----+--------+-------------------+
         | time|count(1)|count(DISTINCT uid)|
         +-----+--------+-------------------+
         |00:02|    1088|                835|
         |00:07|     999|                785|
         |00:06|    1036|                818|
         |00:05|    1024|                809|
         |00:09|     630|                535|
         |00:08|    1024|                783|
         |00:01|    1046|                797|
         |00:03|    1051|                835|
         |00:04|    1056|                830|
         |00:00|    1046|                828|
         +-----+--------+-------------------+*
         */
        //通过算子求解


        //通过sql 求解

        spark.stop();

    }
}
