//package com.example.win;
//
//import org.apache.flink.api.common.functions.AggregateFunction;
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.util.Collector;
//
//import java.util.Arrays;
//
//
///***
// * 增量计算
// */
//public class WindowsFoldApp {
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        // 默认是处理事件
//        //executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//
//        DataStreamSource<String> data = executionEnvironment.socketTextStream("localhost", 9999);
//
//        data
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//                    @Override
//                    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
//                        Arrays.stream(value.split(",")).forEach(o -> {
//                            Integer s = Integer.valueOf(o);
//                            if (s % 2 == 0) {
//                                out.collect(new Tuple2("质数",s));
//                            } else {
//                                out.collect(new Tuple2("偶数",s));
//                            }
//
//                        });
//                    }
//                })
//                .keyBy(0)
//                .timeWindow(Time.seconds(5))
//
//                .print()
//                .setParallelism(1);
//
//        executionEnvironment.execute("a");
//
//    }
//}