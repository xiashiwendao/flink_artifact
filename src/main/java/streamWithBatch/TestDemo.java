package streamWithBatch;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;
import org.apache.flink.table.descriptors.Json;

public class TestDemo {
	public static void main(String[] args) {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
//        env.enableCheckpointing(5000);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

		Kafka kafka = new Kafka().version("0.10").topic("user_behavior")
				.property("bootstrap.servers", "node2.hadoop:9092").property("zookeeper.connect", "node2.hadoop:2181");
//		tableEnv.connect(kafka).withFormat(new Json().failOnMissingField(true).deriveSchema())
//				.withSchema(new Schema().field("user_id", Types.INT).field("item_id", Types.INT)
//						.field("category_id", Types.INT).field("behavior", Types.STRING).field("ts", Types.STRING))
//				.inAppendMode().registerTableSource("tmp_table");

		String sql = "select * from tmp_table";
		Table table = tableEnv.sqlQuery(sql);
		DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);
		rowDataStream.print();
		table.printSchema();

		try {
			env.execute();
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

}
