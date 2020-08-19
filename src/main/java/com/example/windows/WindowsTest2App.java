package com.example.windows;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/***
 * 增量计算
 */
public class WindowsTest2App {


    private static String sdf = "yyyy-MM-dd HH:mm:ss";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        // 默认是处理事件
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每哥五秒 生成一次水位线
        executionEnvironment.getConfig().setAutoWatermarkInterval(Time.seconds(5).toMilliseconds());

        // 测试数据: 每秒钟一条。
// 某个用户在某个时刻浏览了某个商品，以及商品的价值
// {"userID": "user_4", "eventTime": "2019-11-09 10:41:32", "eventType": "browse", "productID": "product_1", "productPrice": 10}

// 用`AllowedLateness` 与 `SideOutputLateData`处理迟到数据

        OutputTag<Mes> lateOutputTag = new OutputTag<Mes>("late-data") {
        };

        SingleOutputStreamOperator<MesRes> result = executionEnvironment
                .socketTextStream("localhost", 9999)

                // 将从Kafka获取的JSON数据解析成Java Bean
                .process(new ProcessFunction<String, Mes>() {
                    @Override
                    public void processElement(String value, Context ctx, Collector<Mes> out) throws Exception {

                        String[] split = value.split(",");
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);
                        Date parse = simpleDateFormat.parse(split[2]);
                        Long t = parse.getTime() / 1000;

                        Mes mes = new Mes();
                        mes.setCrossId(split[0]);
                        mes.setValue(Long.valueOf(split[1]));
                        mes.setTime(t);
                        out.collect(mes);

                        System.out.println("-----正常数据--1---mes=" + mes);
                        out.collect(mes);

                    }
                })
                .setParallelism(1)
                // 提取时间戳生成水印 maxOutOfOrdernessSeconds=0。这样，EventTime即Watermark。
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Mes>() {

                    Long maxOutOfOrderness = Time.minutes(1).toMilliseconds();
                    Long currentMaxTimestamp = 0L;
                    Long lastEmittedWatermark = Long.MIN_VALUE;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        // 允许延迟三秒
                        Long potentialWM = currentMaxTimestamp - maxOutOfOrderness;
                        // 保证水印能依次递增
                        if (potentialWM >= lastEmittedWatermark) {
                            lastEmittedWatermark = potentialWM;
                        }

//                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);
//
//                        String format = simpleDateFormat.format(new Date(lastEmittedWatermark));
//                        System.out.println("lastEmittedWatermark" + format);

                        return new Watermark(lastEmittedWatermark);
                    }

                    @Override
                    public long extractTimestamp(Mes element, long previousElementTimestamp) {

                        currentMaxTimestamp = currentMaxTimestamp > element.getTime() * 1000 ? currentMaxTimestamp : element.getTime() * 1000;

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

                        String format = simpleDateFormat.format(new Date(currentMaxTimestamp));
                        System.out.println("currentMaxTimestamp" + format);

                        return currentMaxTimestamp;
                    }
                })
                .process(new ProcessFunction<Mes, Mes>() {
                    @Override
                    public void processElement(Mes value, Context ctx, Collector<Mes> out) throws Exception {
                        OutputTag<Mes> lateOutputTag = new OutputTag<Mes>("late-data") {
                        };
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

                        long currenttime = ctx.timerService().currentWatermark() / 1000;
//                        if (value.time < currenttime) {
//                            System.out.println("-----迟到数据---2-----mes=" + value + "currenttime=" + simpleDateFormat.format(new Date(currenttime * 1000)));
//
//                            out.collect(value);
//
//                           // ctx.output(lateOutputTag, value);
//                        } else {
                            System.out.println("-----正常数据---2----mes=" + value + "currenttime=" + simpleDateFormat.format(new Date(currenttime * 1000)));
                            out.collect(value);
//                        }

                    }
                })
                // 按用户分组
                .keyBy(Mes::getCrossId)
                // TimeWindow: 10秒
                .timeWindow(Time.minutes(5))
                // allowedLateness: 5秒
                .allowedLateness(Time.minutes(2))
                // 窗口销毁后，依然迟到的数据，可通过Side Output收集起来
                .sideOutputLateData(lateOutputTag)

                // 窗口函数: 获取这段时间内每个用户浏览的所有记录
//                .process(new AllowedLatenessFunction());
        .apply(new WindowFunction<Mes, MesRes, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Mes> input, Collector<MesRes> out) throws Exception {
                // 之后isUpdated被置为true，窗口函数因迟到数据被调用时会进入这里
                long count = 0;
                long v = 0;

                for (Mes in : input) {
                    System.out.println("in--------- " + in);
                    count++;
                    v = v + in.getValue();
                }

                MesRes mesRes = new MesRes();
                mesRes.setCount(count);
                mesRes.setCrossId(s);
                mesRes.setValue(v);
                mesRes.setStart(window.getStart() / 1000);
                mesRes.setEnd(window.getEnd() / 1000);
                out.collect(mesRes);
            }
        });

        // 输出主流
        result.print();

        // 输出窗口销毁后，依然迟到的数据
        // 生产中，可单独将这份数据收集到外部存储
        // result.getSideOutput(lateOutputTag).print();

        executionEnvironment.execute("a");

    }

    public static class AllowedLatenessFunction extends ProcessWindowFunction<Mes, MesRes, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Mes> elements, Collector<MesRes> out) throws Exception {

            ValueState<Boolean> state = context.windowState().getState(new ValueStateDescriptor<>("isUpdated", Boolean.class,false));

            if (state.value()) {
                // 之后isUpdated被置为true，窗口函数因迟到数据被调用时会进入这里
                long count = 0;
                long v = 0;

                for (Mes in : elements) {
                    System.out.println("in--------- " + in);
                    count++;
                    v = v + in.getValue();
                }

                MesRes mesRes = new MesRes();
                mesRes.setCount(count);
                mesRes.setCrossId(s);
                mesRes.setValue(v);
                mesRes.setStart(context.window().getStart() / 1000);
                mesRes.setEnd(context.window().getEnd() / 1000);
                out.collect(mesRes);
            } else {
                long count = 0;
                long v = 0;

                for (Mes in : elements) {
                    System.out.println("in--------- " + in);
                    count++;
                    v = v + in.getValue();
                }

                MesRes mesRes = new MesRes();
                mesRes.setCount(count);
                mesRes.setCrossId(s);
                mesRes.setValue(v);
                mesRes.setStart(context.window().getStart() / 1000);
                mesRes.setEnd(context.window().getEnd() / 1000);
                out.collect(mesRes);

                state.update(true);
            }
        }
    }

    public static class MyProcessWindowFunction extends ProcessWindowFunction<Mes, MesRes, String, TimeWindow> {


        @Override
        public void process(String s, Context context, Iterable<Mes> elements, Collector<MesRes> out) throws Exception {


            long count = 0;
            long v = 0;

            for (Mes in : elements) {
                Long t = in.time;
                System.out.println("in--------- " + in);
                count++;
                v = v + in.getValue();

            }

            MesRes mesRes = new MesRes();
            mesRes.setCount(count);
            mesRes.setCrossId(s);
            mesRes.setValue(v);
            mesRes.setStart(context.window().getStart() / 1000);
            mesRes.setEnd(context.window().getEnd() / 1000);
            out.collect(mesRes);
        }
    }


    public static class MesRes {

        private String crossId;

        private Long value;

        private Long count;

        private Long start;

        private Long end;

        @Override
        public String toString() {


            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

            String s = simpleDateFormat.format(new Date(start * 1000));
            String e = simpleDateFormat.format(new Date(end * 1000));

            return "MesRes{" +
                    "crossId='" + crossId + '\'' +
                    ", value=" + value +
                    ", count=" + count +
                    ", start=" + start + "=" + s +
                    ", end=" + end + "=" + e +
                    '}';
        }

        public MesRes() {
        }

        public MesRes(String crossId, Long value, Long count, Long start, Long end) {
            this.crossId = crossId;
            this.value = value;
            this.count = count;
            this.start = start;
            this.end = end;
        }

        public String getCrossId() {
            return crossId;
        }

        public void setCrossId(String crossId) {
            this.crossId = crossId;
        }

        public Long getValue() {
            return value;
        }

        public void setValue(Long value) {
            this.value = value;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }

        public Long getStart() {
            return start;
        }

        public void setStart(Long start) {
            this.start = start;
        }

        public Long getEnd() {
            return end;
        }

        public void setEnd(Long end) {
            this.end = end;
        }
    }


    public static class Mes {

        private String crossId;

        private Long value;

        private Long time;

        @Override
        public String toString() {

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

            String format = simpleDateFormat.format(new Date(time * 1000));

            return "Mes{" +
                    "crossId='" + crossId + '\'' +
                    ", value=" + value +
                    ", time=" + time +
                    '}' + format;
        }

        public Mes() {
        }

        public Mes(String crossId, Long value, Long time) {
            this.crossId = crossId;
            this.value = value;
            this.time = time;
        }

        public String getCrossId() {
            return crossId;
        }

        public void setCrossId(String crossId) {
            this.crossId = crossId;
        }

        public Long getValue() {
            return value;
        }

        public void setValue(Long value) {
            this.value = value;
        }

        public Long getTime() {
            return time;
        }

        public void setTime(Long time) {
            this.time = time;
        }
    }


}