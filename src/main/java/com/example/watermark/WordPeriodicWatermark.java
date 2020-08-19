package com.example.watermark;

import com.example.pojo.Word;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;


/**
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */
public class WordPeriodicWatermark implements AssignerWithPeriodicWatermarks<Word> {

    private long currentTimestamp = Long.MIN_VALUE;

    private static String sdf = "yyyy-MM-dd HH:mm:ss";

    @Override
    public long extractTimestamp(Word word, long previousElementTimestamp) {

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);


        long timestamp = word.getTimestamp();
        currentTimestamp = currentTimestamp > word.getTimestamp() * 1000 ? currentTimestamp : word.getTimestamp() * 1000;
        System.out.println("event " +
                "timestamp = {" + word.getTimestamp() + "}, {" + simpleDateFormat.format(new Date(timestamp * 1000)) + "}, " +
                "CurrentWatermark = {" + getCurrentWatermark().getTimestamp() + "}, {" + simpleDateFormat.format(new Date(currentTimestamp)) + "}");
        return timestamp * 1000;
    }

    @Nullable
    @Override
    public Watermark getCurrentWatermark() {
        long maxTimeLag = 2000;
        long lastEmittedWatermark = currentTimestamp == Long.MIN_VALUE ? Long.MIN_VALUE : currentTimestamp - maxTimeLag;
//        SimpleDateFormat simpleDateFormat = new SimpleDateFormat(sdf);
//
//        String format = simpleDateFormat.format(new Date(lastEmittedWatermark));
//        System.out.println("lastEmittedWatermark" + format);


        return new Watermark(lastEmittedWatermark);
    }
}
