package com.bugboy.stream;

import com.bugboy.util.TableUtils;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

public class FromElements {
  public static void main(String[] args) throws Exception {
    StreamExecutionEnvironment env =
            StreamExecutionEnvironment.getExecutionEnvironment();
    EnvironmentSettings settings = EnvironmentSettings
            .newInstance()
            .useBlinkPlanner()
            .inStreamingMode()
            .build();
    StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);
    DataStream<Tuple6<String, String, Integer, String, String, String>> ds = env.fromElements(
            new Tuple6<>("0001", "张三", 21, "男", "北京", "保洁"),
            new Tuple6<>("0002", "李四", 22, "女", "上海", "软件工程师"),
            new Tuple6<>("0003", "王五", 23, "男", "杭州", "教师"),
            new Tuple6<>("0004", "赵六", 24, "女", "广州", "化妆师"),
            new Tuple6<>("0005", "朱七", 26, "男", "深圳", "健身教练")
    );

    tEnv.createTemporaryView("persons", ds,
            $("id"),
            $("name"),
            $("age"),
            $("gender"),
            $("address"));
    String sql = "select * from persons";

    Table resultTable = tEnv.sqlQuery(sql);
    for (Row row : TableUtils.collectToList(resultTable)) {
      System.out.println(row);
    }
  }
}
