package org.apache.flink.explain;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.table.TableEnvironment;
import org.apache.flink.api.table.Table;

public class sqlExplainExample {

	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		TableEnvironment tableEnv = new TableEnvironment();
		DataSet<WC> ds = env.fromElements(
				new WC("hello", 1),
				new WC("world", 3),
				new WC("nian", 5));
		Table table = tableEnv.fromDataSet(ds);
		System.out.println(table.groupBy("word")
				.select("word.count as count, word")
				.filter("count = 2").explain());
	}

	public static class WC {
		public String word;
		public int count;
		public WC(String word, int count) {
			this.word = word;
			this.count = count;
		}
		public WC() {}
	}
}
