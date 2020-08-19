package com.example.t1;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class A {
    public static void main(String[] args) {

        List<Integer> l = new ArrayList<>();

        Integer integer = l.stream().reduce((a, b) -> {
            System.out.println("a"+a);
            System.out.println("b"+b);
            int max = Math.max(a, b);
            return max;
        }).orElse(22);

        System.out.println(integer);


    }
}