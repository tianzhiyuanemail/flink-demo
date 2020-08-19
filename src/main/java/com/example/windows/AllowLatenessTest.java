//package com.example.win;
//
//import akka.japi.tuple.Tuple3;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.table.sources.wmstrategies.PunctuatedWatermarkAssigner;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//
//public class AllowLatenessTest {
//
//    /***
//     * Tuple3.of("a", "1", 1551169050000L),
//     * Tuple3.of("aa", "33", 1551169060001L),
//     * Tuple3.of("a", "2", 1551169064000L),
//     * Tuple3.of("a", "3", 1551169059002L),
//     * Tuple3.of("a", "0", 1551169061002L),
//     * Tuple3.of("a", "11", 1551169058002L),
//     * Tuple3.of("b", "5", 15511691067000L),
//     * Tuple3.of("a", "4", 1551169061003L),
//     * Tuple3.of("aa", "44", 1551169079004L),
//     * Tuple3.of("b", "6", 1551169108000L)
//     * @param args
//     * @throws Exception
//     */
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.getConfig().setAutoWatermarkInterval(200);
//        final OutputTag<Tuple3<String, String, Long>> outputTag = new OutputTag<Tuple3<String, String, Long>>("side-output") {
//        };
//
//        DataStream stream =
//                env
//                        .addSource(new StreamDataSource())
//                        .assignTimestampsAndWatermarks(new PunctuatedWatermarkAssigner())
//                        .timeWindowAll(Time.seconds(10))
//                        .allowedLateness(Time.milliseconds(5 * 1000l))
//                        .sideOutputLateData(outputTag)
//                        .apply(new AllWindowFunction<Tuple3<String, String, Long>, Object, TimeWindow>() {
//                            @Override
//                            public void apply(TimeWindow window, Iterable<Tuple3<String, String, Long>> values, Collector<Object> out) throws Exception {
//                                System.out.println("窗口开始时间：" + window.getStart() + ",结束时间" + window.getEnd());
//                                System.out.println("窗口内数据：" + values.toString());
//                            }
//                        })
//                        .getSideOutput(outputTag);
//        stream.print();
//
//        env.execute();
//
//    }
//}
