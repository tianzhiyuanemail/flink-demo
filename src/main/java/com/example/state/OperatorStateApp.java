package com.example.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class OperatorStateApp {

    private static String sdf = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 开启检查点机制
        env.enableCheckpointing(1000);
        // 设置并行度为1
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple3(split[0], Integer.valueOf(split[1]), Integer.valueOf(split[2]));
                    }
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Tuple3<String, Integer, Integer>>() {
                    private long currentTime = 0;
                    private long maxTimeLag = 2000;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentTime - maxTimeLag);
                    }

                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Integer> va, long l) {

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

                        long time = Long.valueOf(va.f2) * 1000;

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
                .reduce(new A())
                .print();

        env.execute("Managed Keyed State");

    }


    public static class A implements ReduceFunction<Tuple3<String, Integer, Integer>>
            , CheckpointedFunction {


        // 非正常数据
        private List<Tuple3<String, Integer, Integer>> bufferedData = new ArrayList<>();
        // checkPointedState
        private transient ListState<Tuple3<String, Integer, Integer>> checkPointedState;
        // 需要监控的阈值
        private Long threshold = 5L;


        @Override
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> value1, Tuple3<String, Integer, Integer> value2) throws Exception {

            Long inputValue = Long.valueOf(value1.f1);
            // 超过阈值则进行记录
            if (inputValue >= threshold) {
                bufferedData.add(value1);
            }
            bufferedData.stream().forEach(o -> System.out.println("阈值警报！" + o));

            return value1;
        }


        @Override
        public void snapshotState(FunctionSnapshotContext functionSnapshotContext) throws Exception {
            // 在进行快照时，将数据存储到checkPointedState
            checkPointedState.clear();
            for (Tuple3<String, Integer, Integer> element : bufferedData) {
                checkPointedState.add(element);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 注意这里获取的是OperatorStateStore
            checkPointedState = context.getOperatorStateStore().
                    getListState(new ListStateDescriptor<>("abnormalData",
                            TypeInformation.of(new TypeHint<Tuple3<String, Integer, Integer>>() {
                            })));
            // 如果发生重启，则需要从快照中将状态进行恢复
            if (context.isRestored()) {
                for (Tuple3<String, Integer, Integer> element : checkPointedState.get()) {
                    bufferedData.add(element);
                }
            }
        }
    }


}