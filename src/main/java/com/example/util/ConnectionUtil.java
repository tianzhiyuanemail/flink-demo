package com.example.util;

import org.apache.flink.streaming.api.windowing.time.Time;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

public class ConnectionUtil {

    public static Integer get(){
        Random random = new Random();
        int i = random.nextInt(10);
        return i;
    }


    public static void set(){

    }

    public static void main(String[] args) throws ParseException {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        String format = simpleDateFormat.format(new Date(1595642520000L));

        System.out.println(format);

    }


}