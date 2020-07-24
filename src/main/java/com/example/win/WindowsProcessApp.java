package com.example.win;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.Arrays;


/***
 * 全量计算
 */
public class WindowsProcessApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认是处理事件
        //executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStreamSource<String> data = executionEnvironment.socketTextStream("localhost", 9999);

        data
                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                        Arrays.stream(value.split(",")).forEach(o -> {
                            Integer s = Integer.valueOf(o);
                            if (s % 2 == 0) {
                                out.collect(new Tuple2("质数", s));
                            } else {
                                out.collect(new Tuple2("偶数", s));
                            }

                        });
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))

                // 一个周期执行一次
                .process(new ProcessWindowFunction<Tuple2<String, Integer>, String, Tuple, TimeWindow>() {
                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {

                        System.out.println("-------------");
                        System.out.println("tuple" + tuple);

                        long count = 0;
                        for (Tuple2<String, Integer> in : elements) {
                            count++;
                        }
                        out.collect("Window: " + context.window() + "count: " + count);
                    }
                })


                .print()
                .setParallelism(1);

        executionEnvironment.execute("a");

    }
}