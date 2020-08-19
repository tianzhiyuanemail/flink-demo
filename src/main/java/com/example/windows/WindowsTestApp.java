package com.example.windows;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Random;

/***
 * 增量计算
 */
public class WindowsTestApp {


    private static String sdf = "yyyy-MM-dd HH:mm:ss";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        final OutputTag<Mes> outputTag = new OutputTag<Mes>("side-output") {
        };

        // 默认是处理事件
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // 每哥五秒 生成一次水位线
        executionEnvironment.getConfig().setAutoWatermarkInterval(Time.seconds(5).toMilliseconds());

        executionEnvironment
                .socketTextStream("localhost", 9999)
                .flatMap(new FlatMapFunction<String, Mes>() {
                    @Override
                    public void flatMap(String value, Collector<Mes> out) throws Exception {
                        String[] split = value.split(",");

                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);

                        Date parse = simpleDateFormat.parse(split[2]);
                        Long t = parse.getTime() / 1000;

                        Mes mes = new Mes();
                        mes.setCrossId(split[0]);
                        mes.setValue(Long.valueOf(split[1]));
                        mes.setTime(t);

                        out.collect(mes);

                    }
                })

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

                        return element.getTime() * 1000;
                    }
                })
                // 设置并行度非常重要  默认情况下 有几个并行度 就有几个 Watermark 只有所有的Watermark都超过刻度线 才会触发聚合
                .setParallelism(1)

                .map(new MapFunction<Mes, Mes>() {
                    @Override
                    public Mes map(Mes value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                })
                 .process(new ProcessFunction<Mes, Mes>() {
                     @Override
                     public void processElement(Mes value, Context ctx, Collector<Mes> out) throws Exception {

                     }
                 })
                .keyBy(Mes::getCrossId)
                .timeWindow(Time.minutes(5))
                .allowedLateness(Time.minutes(2))
                //.sideOutputLateData(outputTag)
                .process(new AllowedLatenessFunction())
                // .setParallelism(1)
                .print();

        executionEnvironment.execute("a");

    }


    public static class AllowedLatenessFunction extends ProcessWindowFunction<Mes, MesRes, String, TimeWindow> {

        @Override
        public void process(String s, Context context, Iterable<Mes> elements, Collector<MesRes> out) throws Exception {

            ValueState<Boolean> state = context.windowState().getState(new ValueStateDescriptor<Boolean>("isUpdated", Boolean.class,false));

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


    public static class MySource implements SourceFunction<Mes> {
        @Override
        public void run(SourceContext<Mes> ctx) throws Exception {
            while (true) {
                Random random = new Random(10);
                int i = random.nextInt(10);
                Mes mes = new Mes();
                mes.setCrossId("lukou-1");
                mes.setTime(System.currentTimeMillis() / 1000 - i * 1000);
                mes.setValue((long) i);
                Thread.sleep(i * 800);
                ctx.collect(mes);
            }
        }

        @Override
        public void cancel() {
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