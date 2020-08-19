package com.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.windowing.RichWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

public class StateWindowsApp {
    private static String sdf = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        executionEnvironment
                .socketTextStream("localhost", 9999)

                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple2(split[0], split[1]);
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple2<String, String>>() {
                    private long currentTime = 0;
                    private long maxTimeLag = 2000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTime - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(Tuple2<String, String> stringStringTuple2, long l) {

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

                        long time = Long.valueOf(stringStringTuple2.f1) * 1000;

                        currentTime = Math.max(time, currentTime);

                        System.out.println("event " +
                                "timestamp = {" + time + "}, {" + simpleDateFormat.format(new Date(time)) + "}, " +
                                "CurrentWatermark = {" + getCurrentWatermark().getTimestamp() + "}, {" + simpleDateFormat.format(new Date(currentTime)) + "}");

                        return time;
                    }
                })
                .keyBy(0)

                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(3))
                .apply(new RichWindowFunction<Tuple2<String, String>, String, Tuple, TimeWindow>() {

                    ValueState<Boolean> valueState = null;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ValueStateDescriptor<Boolean> stateDescriptor = new ValueStateDescriptor<>("a", Boolean.class, false);
                        valueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    @Override
                    public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Tuple2<String, String>> iterable, Collector<String> collector) throws Exception {

                        Boolean b = valueState.value();

                        if (b == false) {
                            for (Tuple2<String, String> t : iterable) {
                                System.out.println("第一次聚合:" + t.f0 + "=" + t.f1);
                            }
                            valueState.update(true);
                        } else {
                            for (Tuple2<String, String> t : iterable) {
                                System.out.println("再次聚合:" + t.f0 + "=" + t.f1);
                            }
                        }
                        collector.collect("聚合l ");
                    }
                })
                .print();


        executionEnvironment.execute("vs");
    }


}