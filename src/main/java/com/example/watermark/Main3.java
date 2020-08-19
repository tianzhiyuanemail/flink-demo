package com.example.watermark;

import com.example.pojo.Word;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.text.SimpleDateFormat;
import java.util.Date;

import static sun.misc.Version.print;

/**
 * Desc: allowedLateness
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class Main3 {
    private static String sdf = "yyyy-MM-dd HH:mm:ss";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //并行度设置为 1
        env.setParallelism(1);
//        env.setParallelism(4);
        // 每哥五秒 生成一次水位线
        //env.getConfig().setAutoWatermarkInterval(Time.seconds(1).toMilliseconds());
        OutputTag<Word> lateDataTag = new OutputTag<Word>("late") {
        };
        SingleOutputStreamOperator<Word> data = env.socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Word>() {
                    @Override
                    public Word map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Word(split[0], Integer.valueOf(split[1]), Long.valueOf(split[2]));
                    }
                }).assignTimestampsAndWatermarks(new WordPeriodicWatermark());

        SingleOutputStreamOperator<Word> sum = data.keyBy(Word::getWord)
                .timeWindow(Time.seconds(10))
                .allowedLateness(Time.seconds(3))
                .sideOutputLateData(lateDataTag)
                .process(new ProcessWindowFunction<Word, Word, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<Word> elements, Collector<Word> out) throws Exception {
                        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);
                        for (Word w : elements) {
                            System.out.println("------out----" + w
                                    + "start=" + simpleDateFormat.format(new Date(context.window().getStart()))
                                    + "  end=" + simpleDateFormat.format(new Date(context.window().getEnd())));
                        }
                    }
                });


        sum.print();

        sum.getSideOutput(lateDataTag).print();


        env.execute("watermark demo");
    }
}
