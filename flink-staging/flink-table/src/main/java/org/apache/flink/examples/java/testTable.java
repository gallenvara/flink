package org.apache.flink.examples.java;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.table.Row;
import org.apache.flink.api.table.Table;


public class testTable {
	public static void main(String[] args) throws Exception {

		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Integer> data = env.fromElements(1, 2, 2, 3, 3, 3, 4, 4, 4, 4);
		DataSet<Tuple2<Integer, Integer>> tuples = data
				.map(new MapFunction<Integer, Tuple2<Integer, Integer>>() {
					@Override
					public Tuple2<Integer, Integer> map(Integer i) throws Exception {
						return new Tuple2<Integer, Integer>(i, i*2);
					}
				});

		TableEnvironment tEnv = new TableEnvironment();
		//Table t = tEnv.fromDataSet(tuples).as("i, i2")
		//		.groupBy("i, i2").select("i, i2")
		//		.groupBy("i").select("i, i.count as cnt");
		Table t = tEnv.fromDataSet(tuples).as("i, i2");

		tEnv.toDataSet(t, Row.class).print();
	}
}
