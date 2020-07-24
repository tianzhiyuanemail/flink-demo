package com.example.datastream;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.List;

public class DataStreamTransformation {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

//        map(executionEnvironment);
//        union(executionEnvironment);
        splitSelect(executionEnvironment);

        executionEnvironment.execute("q");
    }


    private static void map(StreamExecutionEnvironment executionEnvironment) {

        executionEnvironment.addSource(new CustomNonParallelSourceFunction())
                .map(new MapFunction<Long, Long>() {
                    @Override
                    public Long map(Long value) throws Exception {
                        System.out.println("map:" + value);
                        return value;
                    }
                })
                .filter(new FilterFunction<Long>() {
                    @Override
                    public boolean filter(Long value) throws Exception {
                        boolean b = value % 2 == 0;
                        return b;
                    }
                })
                .print();

    }

    private static void union(StreamExecutionEnvironment executionEnvironment) {

        DataStreamSource<Long> d1 = executionEnvironment.addSource(new CustomNonParallelSourceFunction());
        DataStreamSource<Long> d2 = executionEnvironment.addSource(new CustomNonParallelSourceFunction());


        d1.union(d2).print();

    }

    private static void splitSelect(StreamExecutionEnvironment executionEnvironment) {

        DataStreamSource<Long> d1 = executionEnvironment.addSource(new CustomNonParallelSourceFunction());

        SplitStream<Long> split = d1.split(new OutputSelector<Long>() {
            @Override
            public Iterable<String> select(Long value) {

                List<String> list = new ArrayList<>();
                boolean b = value % 2 == 0;

                if (b) {
                    list.add("a");
                } else {
                    list.add("b");
                }
                return list;
            }
        });

        DataStream<Long> a = split.select("a");

        a.print();

    }


}