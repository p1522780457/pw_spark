package com.pangw.test;

import com.pangw.util.JavaRedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestDemo {
    public static void main(String[] args){


        String s = "select * from t_click limit %s,%s,"+1;

        String ss = String.format(s,100,12);

        System.out.println(ss);
    }
}
