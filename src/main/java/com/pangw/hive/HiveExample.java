package com.pangw.hive;

import java.sql.*;

public class HiveExample {
    private String driverName = "org.apache.hive.jdbc.HiveDriver";


    public static void main(String[] args) throws SQLException {
        HiveExample example = new HiveExample();
        example.run();

    }

    private void run() throws SQLException {
        try {
            Class.forName(driverName);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            System.exit(1);
        }

        Connection con = DriverManager.getConnection("jdbc:hive2://bigdata:10000/default", "bigdata", "bigdata");

        Statement stmt = con.createStatement();

        long startTime = System.currentTimeMillis();

        String sql = "select count(*) from record";

        ResultSet res = stmt.executeQuery(sql);
        int count = 0;
        while (res.next()) {
            count++;
            System.out.println(res.getString(1));
        }
        long stopTime = System.currentTimeMillis();

        System.out.println("time:" + (stopTime - startTime) + ", count:" + count);


    }
}
