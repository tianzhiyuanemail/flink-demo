package com.example.datastream;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * ParallelSourceFunction 继承 SourceContext 并没有具体实现什么方法
 *
 * 他为什么就可以并行呢
 *
 * 在创建数据源 addsource时会判断 该类是否属于ParallelSourceFunction 以此来判断
 *
 *
 */
public class CustomParallelSourceFunction implements ParallelSourceFunction<Long> {


    private volatile Boolean runing = true;

    Long count = 1L;


    @Override
    public void run(SourceContext<Long> ctx) throws Exception {

        while (runing) {
            ctx.collect(count);
            count += 1;
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

        runing = false;
    }
}