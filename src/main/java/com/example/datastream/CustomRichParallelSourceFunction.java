package com.example.datastream;

import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 不能并行
 */
public class CustomRichParallelSourceFunction extends RichParallelSourceFunction<Long> {


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