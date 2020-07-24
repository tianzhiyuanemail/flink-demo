package com.example.win;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeutils.base.LongSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.planner.expressions.In;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * 他妈的  没搞懂
 */
public class TriggerApp {
    public static void main(String[] args) throws Exception {
         System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "error");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认是处理事件
//        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

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
                .timeWindowAll(Time.seconds(10))
                .trigger(new MyTrigger())
                .process(new ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple2<String, Integer>> out) throws Exception {

                        Integer c = 0;

                        for (Tuple2<String, Integer> t : elements) {
                            System.out.println("----------" + t);
                            c++;
                        }
                        out.collect(new Tuple2<>("总数", c));
                    }
                })

                .print()
                .setParallelism(1);

        executionEnvironment.execute("b");

    }


    public static class MyTrigger extends Trigger<Tuple2<String, Integer>, TimeWindow> {
        private static final long serialVersionUID = 1L;
        ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("total", Integer.class);

        /***
         * TriggerResult
         * CONTINUE continue :什么都不做。
         * FIRE:触发计算。
         * PURE:清除窗口的元素。
         * FIRE_AND_PURE:触发计算和清除窗口元素。
         */

        //onElement():进入窗口的每个元素都会调用该方法。
        @Override
        public TriggerResult onElement(Tuple2<String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ValueState<Integer> sumState = ctx.getPartitionedState(stateDescriptor);
            if (null == sumState.value()) {
                sumState.update(0);
            }
            Integer c = sumState.value() + 1;
            sumState.update(c);
            if (sumState.value() >= 5) {
                //这里可以选择手动处理状态
                clear(window, ctx);
                System.out.println("长度够了");
                //  默认的trigger发送是TriggerResult.FIRE 不会清除窗口数据
                return TriggerResult.FIRE_AND_PURGE;
            }
            return TriggerResult.CONTINUE;
        }

        //onProcessingTime():处理时间timer触发的时候会被调用。
        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("onProcessingTime 时间够了");
            return TriggerResult.FIRE_AND_PURGE;
        }

        //onEventTime():事件时间timer触发的时候被调用。
        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("onEventTime 时间够了");
            return TriggerResult.CONTINUE;
        }

        //clear():该方法主要是执行窗口的删除操作。
        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            System.out.println("清理窗口状态  窗口内保存值为" + ctx.getPartitionedState(stateDescriptor).value());
            ctx.getPartitionedState(stateDescriptor).clear();
        }

        // onMerge():有状态的触发器相关，并在它们相应的窗口合并时合并两个触发器的状态，例如使用会话窗口。
    }


    /**
     * 带超时的计数窗口触发器
     *
     * @author Zane
     * @date 2019-04-26 15:57
     */
    public static class CountTriggerWithTimeout<T> extends Trigger<T, TimeWindow> {
        private static Logger LOG = LoggerFactory.getLogger(CountTriggerWithTimeout.class);

        /**
         * 窗口最大数据量
         */
        private int maxCount;
        /**
         * event time / process time
         */
        private TimeCharacteristic timeType;
        /**
         * 用于储存窗口当前数据量的状态对象
         */
        private ReducingStateDescriptor<Long> countStateDescriptor =
                new ReducingStateDescriptor("counter", new Sum(), LongSerializer.INSTANCE);


        public CountTriggerWithTimeout(int maxCount, TimeCharacteristic timeType) {

            this.maxCount = maxCount;
            this.timeType = timeType;
        }


        private TriggerResult fireAndPurge(TimeWindow window, TriggerContext ctx) throws Exception {
            clear(window, ctx);
            return TriggerResult.FIRE_AND_PURGE;
        }


        @Override
        public TriggerResult onElement(T element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
            countState.add(1L);

            if (countState.get() >= maxCount) {
                LOG.info("fire with count: " + countState.get());
                return fireAndPurge(window, ctx);
            }
            if (timestamp >= window.getEnd()) {
                LOG.info("fire with tiem: " + timestamp);
                return fireAndPurge(window, ctx);
            } else {
                return TriggerResult.CONTINUE;
            }
        }

        @Override
        public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (timeType != TimeCharacteristic.ProcessingTime) {
                return TriggerResult.CONTINUE;
            }

            if (time >= window.getEnd()) {
                return TriggerResult.CONTINUE;
            } else {
                LOG.info("fire with process tiem: " + time);
                return fireAndPurge(window, ctx);
            }
        }

        @Override
        public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
            if (timeType != TimeCharacteristic.EventTime) {
                return TriggerResult.CONTINUE;
            }

            if (time >= window.getEnd()) {
                return TriggerResult.CONTINUE;
            } else {
                LOG.info("fire with event tiem: " + time);
                return fireAndPurge(window, ctx);
            }
        }

        @Override
        public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
            ReducingState<Long> countState = ctx.getPartitionedState(countStateDescriptor);
            countState.clear();
        }

    }


    /**
     * 计数方法
     */
    public static class Sum implements ReduceFunction<Long> {

        @Override
        public Long reduce(Long value1, Long value2) throws Exception {
            return value1 + value2;
        }
    }


}
