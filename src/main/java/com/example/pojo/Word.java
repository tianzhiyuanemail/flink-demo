package com.example.pojo;


/**
 * Desc:
 * Created by zhisheng on 2019-07-07
 * blog：http://www.54tianzhisheng.cn/
 * 微信公众号：zhisheng
 */

public class Word {
    private String word;
    private int count;
    private long timestamp;

    @Override
    public String toString() {
        return "Word{" +
                "word='" + word + '\'' +
                ", count=" + count +
                ", timestamp=" + timestamp +
                '}';
    }

    public String getWord() {
        return word;
    }

    public void setWord(String word) {
        this.word = word;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Word() {
    }

    public Word(String word, int count, long timestamp) {
        this.word = word;
        this.count = count;
        this.timestamp = timestamp;
    }
}
