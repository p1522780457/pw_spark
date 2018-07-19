package com.weiboanalyze.sparksql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class WeiboSparkSqlAnalyze {

    public static String path_follower = "data/weibo/follower_followee.csv";
    public static String path_user = "/Users/pangw/pw_workspace/IdeaProjects/pw_spark/data/weibo/weibo_user.csv";

    public static void main (String[] args){
        SparkSession spark = SparkSession.builder()
                .master("local")
                .appName("weiboAnalyze")
                .getOrCreate();
        Dataset<Row>  followers = spark.read().option("header","true").csv(path_follower);

        Dataset<Row> followers01 = followers.select("id","follower_id","followee_id");

        followers01.show();




        
    }
}
