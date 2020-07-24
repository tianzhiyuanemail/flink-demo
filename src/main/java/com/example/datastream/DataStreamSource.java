package com.example.datastream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class DataStreamSource {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        socket(executionEnvironment);
//        customNonParallelSourceFunction(executionEnvironment);
//        customParallelSourceFunction(executionEnvironment);
        richCustomParallelSourceFunction(executionEnvironment);
//        file(executionEnvironment);

        System.out.println("--------------");
        executionEnvironment.execute("job");
    }


    private static void richCustomParallelSourceFunction(StreamExecutionEnvironment executionEnvironment) {
        CustomRichParallelSourceFunction richParallelSourceFunction = new CustomRichParallelSourceFunction();
        executionEnvironment
                .addSource(richParallelSourceFunction)
                .setParallelism(5)
                .print();
    }

    private static void customParallelSourceFunction(StreamExecutionEnvironment executionEnvironment) {
        CustomParallelSourceFunction customParallelSourceFunction = new CustomParallelSourceFunction();
        executionEnvironment
                .addSource(customParallelSourceFunction)
//                .map(new MapFunction<Long, Integer>() {
//                    @Override
//                    public Integer map(Long value) throws Exception {
//                        Integer s = Math.toIntExact(value % 2);
//                        return s;
//                    }
//                })
                .setParallelism(5)
                .print();
    }

    private static void customNonParallelSourceFunction(StreamExecutionEnvironment executionEnvironment) {

        executionEnvironment
                .addSource(new CustomNonParallelSourceFunction())
//                .map(new MapFunction<Long, Integer>() {
//                    @Override
//                    public Integer map(Long value) throws Exception {
//                        Integer s = Math.toIntExact(value % 2);
//                        return s;
//                    }
//                })
                .print();
    }


    private static void file(StreamExecutionEnvironment executionEnvironment) {
        String s = "file:///Users/baidu/Desktop/111.txt";
        executionEnvironment.readTextFile(s)
                .setParallelism(1)
                .print();
    }


    private static void socket(StreamExecutionEnvironment executionEnvironment) {
        executionEnvironment.socketTextStream("localhost", 8091)
//                .flatMap(new FlatMapFunction<String, Word>() {
//                    @Override
//                    public void flatMap(String value, Collector<Word> out) throws Exception {
//                        Arrays.stream(value.split(",")).forEach(o -> out.collect(new Word(o, 1)));
//                    }
//                })
//                .keyBy(Word::getName)
//                .timeWindow(Time.seconds(5))
//                .sum("count")
                .setParallelism(1)
                .print();
    }

    /**
     * 该类是公有且独立的（没有非静态内部类）
     * 该类有公有的无参构造函数
     * 类（及父类）中所有的所有不被 static、transient 修饰的属性要么是公有的（且不被 final 修饰），
     * 要么是包含公有的 getter 和 setter 方法，这些方法遵循 Java bean 命名规范。
     */
    public static class Word {
        private String name;
        private Integer count;

        @Override
        public String toString() {
            return "Word{" +
                    "name='" + name + '\'' +
                    ", count=" + count +
                    '}';
        }

        public Word() {
        }

        public Word(String name, Integer count) {
            this.name = name;
            this.count = count;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }
    }


}