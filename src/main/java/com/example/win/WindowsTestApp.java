package com.example.win;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Random;

/***
 * 增量计算
 */
public class WindowsTestApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 默认是处理事件
        executionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


        executionEnvironment
                .addSource(new SourceFunction<Mes>() {
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
                })
                .assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Mes>() {

                    Long maxOutOfOrderness = 3000L; // 3 seconds
                    Long currentMaxTimestamp = 0L;

                    @Nullable
                    @Override
                    public Watermark getCurrentWatermark() {
                        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
                    }

                    @Override
                    public long extractTimestamp(Mes element, long previousElementTimestamp) {

                        currentMaxTimestamp = currentMaxTimestamp > element.getTime() * 1000 ? currentMaxTimestamp : element.getTime() * 1000;

                        return currentMaxTimestamp;
                    }
                })
                .map(new MapFunction<Mes, Mes>() {
                    @Override
                    public Mes map(Mes value) throws Exception {
                        System.out.println(value);
                        return value;
                    }
                })
                .keyBy(Mes::getCrossId)
                .timeWindow(Time.seconds(10))
                .process(new ProcessWindowFunction<Mes, MesRes, String, TimeWindow>() {
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
                        mesRes.setStart(context.window().getStart()/1000);
                        mesRes.setEnd(context.window().getEnd()/1000);
                        out.collect(mesRes);
                    }
                })
                .print();

        executionEnvironment.execute("a");

    }

    public static class MesRes {

        private String crossId;

        private Long value;

        private Long count;

        private Long start;

        private Long end;

        @Override
        public String toString() {


            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

            String s = simpleDateFormat.format(new Date(start * 1000));
            String e = simpleDateFormat.format(new Date(end * 1000));

            return "MesRes{" +
                    "crossId='" + crossId + '\'' +
                    ", value=" + value +
                    ", count=" + count +
                    ", start=" + start +"="+ s +
                    ", end=" + end +"="+ e +
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

            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("YYYY-MM-dd HH:mm:ss");

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