package org.example;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<Person> personStream = env.fromElements(
                new Person("Fred", 35),
                new Person("Jack", 36),
                new Person("Bill", 3));

        DataStream<Person> adultStream = personStream
                .filter((FilterFunction<Person>) person -> person.age > 19);

        adultStream.print();
        env.execute();

        // flink run -c org.example.DataStreamExample target/flink-example-1.0.0.jar
    }

    public static class Person {
        public String name;
        public Integer age;

        public Person() {}

        public Person(String name, Integer age) {
            this.name = name;
            this.age = age;
        }

        @Override
        public String toString() {
            return this.name + ": age " + this.age.toString();
        }
    }
}
