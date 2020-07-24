package com.example.dataset;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

public class DataSetSourceApp {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        // 1 从list 获取数据
        //DataSource<Object> objectDataSource = environment.fromCollection(new ArrayList<>());

        // 2  自定义方式
        //DataSource<String> stringDataSource = environment.fromElements("a,b,d,s,a,a,b");

        // 3  读取文件
//        DataSource<String> stringDataSource = environment.readTextFile("file:///Users/baidu/Desktop/111.txt");


//        DataSource<Peo> peoDataSource = environment
//                .readCsvFile("file:///Users/baidu/Desktop/csv.csv")
//                .ignoreFirstLine()
//                .pojoType(Peo.class, "name", "age", "job");

        // 读取压缩文件
        Configuration parameters = new Configuration();
        parameters.setBoolean("recursive.file.enumeration", true);
        DataSource<String> stringDataSource = environment.readTextFile("file:///Users/baidu/Desktop/111.txt.zip")
//                .withParameters(parameters)
                ;

        stringDataSource.print();
        System.out.println("-------------------------------------------------------------------------");

    }


    public static class Peo {
        private String name;
        private Integer age;
        private String job;

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

        public String getJob() {
            return job;
        }

        public void setJob(String job) {
            this.job = job;
        }

        @Override
        public String toString() {
            return "Peo{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", job='" + job + '\'' +
                    '}';
        }
    }

    /**
     ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

     // read text file from local files system
     DataSet<String> localLines = env.readTextFile("file:///path/to/my/textfile");

     // read text file from an HDFS running at nnHost:nnPort
     DataSet<String> hdfsLines = env.readTextFile("hdfs://nnHost:nnPort/path/to/my/textfile");

     // read a CSV file with three fields
     DataSet<Tuple3<Integer, String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     .types(Integer.class, String.class, Double.class);

     // read a CSV file with five fields, taking only two of them
     DataSet<Tuple2<String, Double>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     .includeFields("10010")  // take the first and the fourth field
     .types(String.class, Double.class);

     // read a CSV file with three fields into a POJO (Person.class) with corresponding fields
     DataSet<Person>> csvInput = env.readCsvFile("hdfs:///the/CSV/file")
     .pojoType(Person.class, "name", "age", "zipcode");

     // read a file from the specified path of type SequenceFileInputFormat
     DataSet<Tuple2<IntWritable, Text>> tuples =
     env.createInput(HadoopInputs.readSequenceFile(IntWritable.class, Text.class, "hdfs://nnHost:nnPort/path/to/file"));

     // creates a set from some given elements
     DataSet<String> value = env.fromElements("Foo", "bar", "foobar", "fubar");

     // generate a number sequence
     DataSet<Long> numbers = env.generateSequence(1, 10000000);

     // Read data from a relational database using the JDBC input format
     DataSet<Tuple2<String, Integer> dbData =
     env.createInput(
     JdbcInputFormat.buildJdbcInputFormat()
     .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
     .setDBUrl("jdbc:derby:memory:persons")
     .setQuery("select name, age from persons")
     .setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO))
     .finish()
     );

     */


}