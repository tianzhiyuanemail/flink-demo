package com.example.t1;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WCBatch {


    public static void main(String[] args) throws Exception {

        String s = "file:///Users/baidu/Desktop/111.txt";


        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> stringDataSource = environment.readTextFile(s);


        stringDataSource
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
                        String[] split = value.toLowerCase().split("\t");
                        for (String t : split) {
                            out.collect(new Tuple2<>(t, 1));
                        }
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();
//
//        (str, c) -> {
//                    System.out.println(str);
//                    String[] split = str.toLowerCase().split("\t");
//                    for (String t : split) {
//                        c.collect(new Tuple2<String, Integer>(t, 1));
//                    }
//                })
//                .groupBy(0)
//                .sum(1)
//                .print();

    }

}