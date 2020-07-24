package com.example.dataset;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

public class CountApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();
        String s = "file:///Users/baidu/Desktop/222";

        environment.generateSequence(0, 10)
                .map(new RichMapFunction<Long, Long>() {

                    IntCounter intCounter = new IntCounter(0);

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        super.open(parameters);
                        getRuntimeContext().addAccumulator("c", intCounter);
                    }

                    @Override
                    public Long map(Long value) throws Exception {
                        intCounter.add(1);
                        return value;
                    }
                })
                .setParallelism(5)
                .writeAsText(s, FileSystem.WriteMode.OVERWRITE);


        JobExecutionResult s1 = environment.execute("s");
        Object c1 = s1.getAccumulatorResult("c");

        System.out.println(c1);
        System.out.println("---------");

//        AtomicInteger s = new AtomicInteger(1);
//        environment.generateSequence(0, 10)
//                .map(new MapFunction<Long, Long>() {
//                    @Override
//                    public Long map(Long value) throws Exception {
//                        s.incrementAndGet();
//                        System.out.println("count=" + s);
//                        return value;
//                    }
//                })
//                .setParallelism(5)
//                .print();


    }


}