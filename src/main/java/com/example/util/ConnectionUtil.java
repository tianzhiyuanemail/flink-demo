package com.example.util;

import java.util.Random;

public class ConnectionUtil {

    public static Integer get(){
        Random random = new Random();
        int i = random.nextInt(10);
        return i;
    }


    public static void set(){

    }

    public static void main(String[] args) {
        System.out.println(get());
    }


}