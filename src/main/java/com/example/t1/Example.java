package com.example.t1;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Example {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> flintstones = env.fromElements(
                new Person("Fred", 35),
                new Person("Wilma", 35),
                new Person("Pebbles", 2));

        DataStream<Person> adults = flintstones.filter(person -> {
            return person.age >= 18;
        });

        adults.map(m->{

            System.out.println(m.getName());

            return null;
        });

        env.execute();
    }


    /**
     * 该类是公有且独立的（没有非静态内部类）
     * 该类有公有的无参构造函数
     * 类（及父类）中所有的所有不被 static、transient 修饰的属性要么是公有的（且不被 final 修饰），
     *      要么是包含公有的 getter 和 setter 方法，这些方法遵循 Java bean 命名规范。
     */
    public static class Person {
        public String name;
        public Integer age;

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }
    }
}