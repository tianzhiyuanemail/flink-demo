package com.example.dataset;

import com.example.util.ConnectionUtil;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DataSetTransFormation {

    public static void main(String[] args) throws Exception {


        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

//        map(environment);
//        filter(environment);
//        mapPartition(environment);
//        first(environment);
//        flatmap(environment);
//        distinct(environment);
//        join(environment);
//        cross(environment);
//        reduce(environment);
        max(environment);

        System.out.println("-------------");


    }

    private static void filter(ExecutionEnvironment environment) throws Exception {
        DataSource<String> stringDataSource =
                environment.fromElements("3", "4", "5", "6", "7", "8", "9", "10", "11", "12");

        stringDataSource
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.valueOf(value);
                    }
                })
                .filter(new FilterFunction<Integer>() {
                    @Override
                    public boolean filter(Integer value) throws Exception {
                        return value % 2 == 0;
                    }
                })
                .print();
    }

    private static void map(ExecutionEnvironment environment) throws Exception {
        DataSource<String> stringDataSource =
                environment.fromElements("3", "4", "5", "6", "7", "8", "9", "10", "11", "12");

        stringDataSource
                // map 一对一 转换
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {

                        Integer integer = Integer.valueOf(value);

                        return integer % 2;
                    }
                })
                .print();
    }

    private static void reduce(ExecutionEnvironment environment) throws Exception {
        DataSource<String> stringDataSource =
                environment.fromElements("3", "4", "5", "6");

        stringDataSource
                // map 一对一 转换
                .map(new MapFunction<String, Integer>() {
                    @Override
                    public Integer map(String value) throws Exception {

                        Integer integer = Integer.valueOf(value);

                        return integer;
                    }
                })
//                .reduce(new ReduceFunction<Integer>() {
//                    @Override
//                    public Integer reduce(Integer value1, Integer value2) throws Exception {
//
//                        // v1 是两数相减的结果  v2 是下一个值
//                        System.out.println("v1=" + value1 + "  v2=" + value2);
//                        return value1 - value2;
//                    }
//                })
//                .reduceGroup(new GroupReduceFunction<Integer, Integer>() {
//                    @Override
//                    public void reduce(Iterable<Integer> values, Collector<Integer> out) {
//                        int prefixSum = 0;
//                        // values 元数据 3 4 5 6
//                        for (Integer i : values) {
//                            System.out.println("====" + i);
//                            prefixSum += i;
//                            out.collect(prefixSum);
//                        }
//                    }
//                })
                .print();
    }


    private static void max(ExecutionEnvironment environment) throws Exception {
        DataSource<Tuple3<Integer, Integer, Integer>> stringDataSource1 =
                environment.fromElements(
                        new Tuple3(1, 1, 1),
                        new Tuple3(1, 2, 6),
                        new Tuple3(1, 3, 3),
                        new Tuple3(3, 3, 2),
                        new Tuple3(4, 4, 1)
                );

        stringDataSource1
//                .max(0)
//                .maxBy(0)
//                .groupBy(0)
//                .reduceGroup(new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Object>() {
//                    @Override
//                    public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> values, Collector<Object> out) throws Exception {
//
//                    }
//                })
                .sum(1)
                // .min(2)
                .print();


    }


    private static void join(ExecutionEnvironment environment) throws Exception {
        DataSource<Tuple2<Integer, String>> stringDataSource1 =
                environment.fromElements(
                        new Tuple2(1, "小王"),
                        new Tuple2(2, "小里"),
                        new Tuple2(3, "小张"),
                        new Tuple2(4, "小四")
                );


        DataSource<Tuple2<Integer, String>> stringDataSource2 =
                environment.fromElements(
                        new Tuple2(1, "北京"),
                        new Tuple2(2, "上海"),
                        new Tuple2(3, "成都"),
                        new Tuple2(5, "重庆")
                );


        stringDataSource1
                .join(stringDataSource2)
                //.leftOuterJoin(stringDataSource2)
                //.rightOuterJoin(stringDataSource2)
                //.fullOuterJoin(stringDataSource2)
                .where(0)
                .equalTo(0)


                // 自定义 返回值
//                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, List<String>>() {
//                    @Override
//                    public List<String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
//
//                        List<String> collector =  new ArrayList<>() ;
//                        collector.add(first.f1);
//                        collector.add(second.f1);
//
//                        return collector;
//                    }
//                })
//                .flatMap(new FlatMapFunction<List<String>, String>() {
//                    @Override
//                    public void flatMap(List<String> value, Collector<String> out) throws Exception {
//                        value.stream().forEach(o->out.collect(o));
//                    }
//                })
                .with(new JoinFunction<Tuple2<Integer, String>, Tuple2<Integer, String>, Tuple3<Integer, String, String>>() {
                    @Override
                    public Tuple3<Integer, String, String> join(Tuple2<Integer, String> first, Tuple2<Integer, String> second) throws Exception {
                        if (first == null) {
                            return new Tuple3<>(second.f0, "-", second.f1);
                        } else if (second == null) {
                            return new Tuple3<>(first.f0, first.f1, "-");
                        } else {
                            return new Tuple3<>(first.f0, first.f1, second.f1);
                        }
                    }
                })
                .print();


    }

    private static void cross(ExecutionEnvironment environment) throws Exception {
        DataSource<Tuple1<Integer>> stringDataSource1 =
                environment.fromElements(
                        new Tuple1(1),
                        new Tuple1(2)
                );

        DataSource<Tuple1<String>> stringDataSource2 =
                environment.fromElements(
                        new Tuple1("北京"),
                        new Tuple1("重庆")
                );

        stringDataSource1.cross(stringDataSource2).with(new CrossFunction<Tuple1<Integer>, Tuple1<String>, Tuple2<Integer, String>>() {
            @Override
            public Tuple2<Integer, String> cross(Tuple1<Integer> val1, Tuple1<String> val2) throws Exception {
                return new Tuple2(val1.f0, val2.f0);
            }
        }).print();


    }

    private static void distinct(ExecutionEnvironment environment) throws Exception {
        DataSource<String> stringDataSource =
                environment.fromElements("a", "a", "a", "b", "b", "b", "c", "c", "d", "f");

        stringDataSource.distinct().print();
    }

    private static void flatmap(ExecutionEnvironment environment) throws Exception {
        DataSource<String> stringDataSource =
                environment.fromElements("a,b", "c,d", "s,v,a,b", "a,b,c,d,s,", "w,s");

        stringDataSource
                // map 一对一 转换
                .flatMap(new FlatMapFunction<String, String>() {
                    @Override
                    public void flatMap(String value, Collector<String> out) throws Exception {
                        Arrays.stream(value.split(",")).forEach(out::collect);
                    }
                })
                .map(new MapFunction<String, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> map(String value) throws Exception {
                        return new Tuple2<>(value, 1);
                    }
                })
                .groupBy(0)
                .sum(1)
                .print();
    }

    private static void first(ExecutionEnvironment environment) throws Exception {
        DataSource<Tuple2<Integer, String>> stringDataSource =
                environment.fromElements(
                        new Tuple2(1, "a"),
                        new Tuple2(1, "b"),
                        new Tuple2(1, "c"),
                        new Tuple2(1, "d"),
                        new Tuple2(2, "a"),
                        new Tuple2(2, "b"),
                        new Tuple2(3, "c"),
                        new Tuple2(4, "d"),
                        new Tuple2(5, "e")
                );

        // 取全部集合的前3个
        // stringDataSource.first(3).print();

        // 分组后 取分组内前两个
        // stringDataSource.groupBy(0).first(2).print();

        // 分组后 取组内前三个 并且按照第二个地段 正序排序
        stringDataSource.groupBy(0).sortGroup(1, Order.ASCENDING).first(3).print();

    }

    private static void mapPartition(ExecutionEnvironment environment) throws Exception {
        DataSource<Long> stringDataSource =
                environment.generateSequence(0, 10);

        stringDataSource
                //  转换一个分区的数据
                .mapPartition(new MapPartitionFunction<Long, Long>() {
                    @Override
                    public void mapPartition(Iterable<Long> values, Collector<Long> out) throws Exception {
                        Integer integer = ConnectionUtil.get();
                        System.out.println("ConnectionUtil:" + integer);
                        ConnectionUtil.set();

                        values.forEach(s -> out.collect(s));
                    }
                })
                // 设置并行度
                .setParallelism(6)
//                .map(new MapFunction<Long, Object>() {
//                    @Override
//                    public Object map(Long value) throws Exception {
//
//                        Integer integer = ConnectionUtil.get();
//
//                        System.out.println("ConnectionUtil:" + integer);
//
//                        ConnectionUtil.set();
//
//                        return value;
//                    }
//                })
                .print();

    }

}