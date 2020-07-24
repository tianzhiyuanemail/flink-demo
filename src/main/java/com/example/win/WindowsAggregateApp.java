package com.example.win;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;



/***
 * 增量计算
 */
public class WindowsAggregateApp {

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
                                out.collect(new Tuple2("质数",s));
                            } else {
                                out.collect(new Tuple2("偶数",s));
                            }

                        });
                    }
                })
                .keyBy(0)
                .timeWindow(Time.seconds(5))

                // 一个周期执行一次
//                .process(new ProcessWindowFunction<Tuple2<Integer, Integer>, Object, Tuple, TimeWindow>() {
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple2<Integer, Integer>> elements, Collector<Object> out) throws Exception {
//
//                        System.out.println("-------------");
//                        System.out.println("tuple" + tuple);
//
//                        long count = 0;
//                        for (Tuple2<Integer, Integer> in : elements) {
//                            count++;
//                        }
//                        out.collect("Window: " + context.window() + "count: " + count);
//                    }
//                })

                /**
                 * Tuple2<Integer, Integer> 入参
                 * Tuple2<Integer, Integer> 累加器
                 * Integer 出参
                 */
                .aggregate(new AggregateFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {

                    //createAccumulator：创建一个新的累加器，开始一个新的聚合。累加器是正在运行的聚合的状态。
                    @Override
                    public Tuple3<String, Integer, Integer> createAccumulator() {
                        System.out.println("-------------createAccumulator------");
                        return new Tuple3<>("", 0, 0);
                    }

                    /***
                     * add：将给定的输入添加到给定的累加器，并返回新的累加器值。
                     *
                     * @param value 每次迭代的数据 （1，1）  （1，2）  （1，3）  （1，4）
                     * @param accumulator 累加后的数据  （1，1）  （2，3）  （3，6）  （4，9） （key,次数,总和）
                     * @return
                     */
                    @Override
                    public Tuple3<String, Integer, Integer> add(Tuple2<String, Integer> value, Tuple3<String, Integer, Integer> accumulator) {
                        System.out.println("-------------add------");
                        // keyby
                        accumulator.f0 = value.f0;
                        // 次数
                        accumulator.f1++;
                        // 累加值
                        accumulator.f2 = value.f1 + accumulator.f2;
                        return accumulator;
                    }

                    /***
                     * getResult：从累加器获取聚合结果。
                     * @param accumulator 累加器
                     * @return
                     */
                    @Override
                    public Tuple2<String, Integer> getResult(Tuple3<String, Integer, Integer> accumulator) {
                        System.out.println("-------------getResult------");
                        Integer i = accumulator.f2 / accumulator.f1;

                        // 聚合后的值  分组ID keyby
                        Tuple2<String, Integer> tuple2 = new Tuple2<>(accumulator.f0, i);

                        return tuple2;
                    }

                    //merge：合并两个累加器，返回合并后的累加器的状态。
                    @Override
                    public Tuple3<String, Integer, Integer> merge(Tuple3<String, Integer, Integer> a, Tuple3<String, Integer, Integer> b) {
                        System.out.println("-------------merge------");
                        Tuple3<String, Integer, Integer> tuple3 = new Tuple3<>(a.f0,a.f1 + b.f1, a.f2 + b.f2);
                        return tuple3;
                    }
                })

                .print()
                .setParallelism(1);

        executionEnvironment.execute("a");

    }
}