package com.example.state;

import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;

public class KeyStateApp {

    public static void main(String[] args) throws Exception {






        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        executionEnvironment
                .socketTextStream("localhost", 9999)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    @Override
                    public Tuple2 map(String value) throws Exception {
                        String[] split = value.split(",");
                        return new Tuple2(split[0], split[1]);
                    }
                })
                .keyBy(0)
                //.map(new MyValueState())
                //.map(new MyListValueState())
                //.map(new MyMapValueState())
                //.map(new MyReducingState())
                .map(new MyAggregatingState())
                .print();


        executionEnvironment.execute("vs");
    }


    public static class MyValueState extends RichMapFunction<Tuple2<String, String>, String> {

        StateTtlConfig ttlConfig = StateTtlConfig
                // 设置有效期为 10 秒
                .newBuilder(Time.seconds(10))
                // 设置有效期更新规则，这里设置为当创建和写入时，都重置其有效期到规定的10秒
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                /*设置只要值过期就不可见，另外一个可选值是ReturnExpiredIfNotCleanedUp，
                 代表即使值过期了，但如果还没有被物理删除，就是可见的*/
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        ValueState<Integer> valueState = null;



        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("a", Integer.class);
            stateDescriptor.enableTimeToLive(ttlConfig);

            valueState = getRuntimeContext().getState(stateDescriptor);

            //This method should not be called outside of a keyed context.
            //valueState.update(0);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer integer = Integer.valueOf(value.f1);
            if (integer > 5) {
                Integer value1 = valueState.value() == null ? 0 : valueState.value();
                valueState.update(value1 + 1);
                System.out.println("integer > 5 的次数为=" + valueState.value());
            }
            return value.f0 + "------" + value.f1;
        }
    }

    public static class MyListValueState extends RichMapFunction<Tuple2<String, String>, String> {

        ListState<Integer> listState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("a", Integer.class);
            listState = getRuntimeContext().getListState(stateDescriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer integer = Integer.valueOf(value.f1);
            if (integer > 5) {
                listState.add(integer);
            }
            for (Integer i : listState.get()) {
                System.out.println("integer > 5 的数为=" + i);
            }

            return value.f0 + "------" + value.f1;
        }
    }

    public static class MyMapValueState extends RichMapFunction<Tuple2<String, String>, String> {

        MapState<Integer, Integer> mapState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<Integer, Integer> stateDescriptor = new MapStateDescriptor<Integer, Integer>("a", Integer.class, Integer.class);
            mapState = getRuntimeContext().getMapState(stateDescriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer integer = Integer.valueOf(value.f1);
            if (integer > 5) {
                mapState.put(integer, integer);
            }

            for (Map.Entry<Integer, Integer> i : mapState.entries()) {
                System.out.println("integer > 5 的数为--" + i.getKey() + "==" + i.getValue());
            }

            return value.f0 + "------" + value.f1;
        }
    }

    public static class MyReducingState extends RichMapFunction<Tuple2<String, String>, String> {

        ReducingState<Integer> reducingState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            ReducingStateDescriptor<Integer> stateDescriptor = new ReducingStateDescriptor<Integer>("a", new ReduceFunction<Integer>() {
                @Override
                public Integer reduce(Integer value1, Integer value2) throws Exception {
                    System.out.println("v1=" + value1);
                    System.out.println("v2=" + value2);
                    return value1 + value2;
                }
            }, Integer.class);
            reducingState = getRuntimeContext().getReducingState(stateDescriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer integer = Integer.valueOf(value.f1);
            if (integer > 5) {
                reducingState.add(integer);
            }

            System.out.println("integer > 5 的和为--" + reducingState.get());

            return value.f0 + "------" + value.f1;
        }
    }

    public static class MyAggregatingState extends RichMapFunction<Tuple2<String, String>, String> {

        AggregatingState<Integer, Integer> aggregatingState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            AggregatingStateDescriptor<Integer, Integer, Integer> stateDescriptor = new AggregatingStateDescriptor<Integer, Integer, Integer>("a", new AggregateFunction<Integer, Integer, Integer>() {
                @Override
                public Integer createAccumulator() {
                    return new Integer(0);
                }

                /***
                 *
                 * @param value 本次需要添加的值
                 * @param accumulator 已经聚合的值  英 [əˈkjuːmjəleɪtə(r)] 累加器
                 * @return
                 */
                @Override
                public Integer add(Integer value, Integer accumulator) {
                    System.out.println("add value=" + value);
                    System.out.println("add accumulator=" + accumulator);
                    return value + accumulator;
                }

                @Override
                public Integer getResult(Integer accumulator) {
                    return accumulator;
                }

                @Override
                public Integer merge(Integer a, Integer b) {
                    System.out.println("merge a=" + a);
                    System.out.println("merge b=" + b);
                    return a + b;
                }
            }, Integer.class);
            aggregatingState = getRuntimeContext().getAggregatingState(stateDescriptor);
        }

        @Override
        public String map(Tuple2<String, String> value) throws Exception {
            Integer integer = Integer.valueOf(value.f1);
            if (integer > 5) {
                aggregatingState.add(integer);
            }

            System.out.println("integer > 5 的和为--" + aggregatingState.get());

            return value.f0 + "------" + value.f1;
        }
    }

}