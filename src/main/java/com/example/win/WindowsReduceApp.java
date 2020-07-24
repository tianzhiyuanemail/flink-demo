package com.example.win;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/***
 * 增量计算
 */
public class WindowsReduceApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认是处理事件
        //executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> data = executionEnvironment.socketTextStream("localhost", 9999);

        data
                .flatMap(new FlatMapFunction<String, Tuple2<Integer, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<Integer, Integer>> out) throws Exception {
                        Arrays.stream(value.split(",")).forEach(o -> out.collect(new Tuple2(Integer.valueOf(o), 1)));
                    }
                })
                .keyBy(1)
                .timeWindow(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<Integer, Integer>>() {
                    @Override
                    public Tuple2<Integer, Integer> reduce(Tuple2<Integer, Integer> value1, Tuple2<Integer, Integer> value2) throws Exception {

                        System.out.println(value1 + "=======" + value2);
                        return new Tuple2<>(value1.f0+value2.f0, value1.f1);
                    }
                })

                .print()
                .setParallelism(1);

        executionEnvironment.execute("a");

    }
}