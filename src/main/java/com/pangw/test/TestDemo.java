package com.pangw.test;

import com.pangw.util.JavaRedisClient;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

public class TestDemo {
    public static void main(String[] args){
        System.out.println("fsdfsdfsd");
        Jedis jedis = JavaRedisClient.get().getResource();
       jedis.hincrBy("myhash","id",666l);
       System.out.println(jedis.hincrBy("myhash","password",1l));


    }
}
