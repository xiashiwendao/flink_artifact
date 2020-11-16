package streamWithBatch;

import java.time.Duration;

import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.types.Row;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.OutputTag;

public class FlinkToHive {

	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		streamEnv.setParallelism(5);
		streamEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		streamEnv.getConfig().setAutoWatermarkInterval(120000);
		EnvironmentSettings tableEnvSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode()
				.build();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(streamEnv, tableEnvSettings);
		String category_name = "hive_category";
		HiveCatalog catalog = new HiveCatalog(category_name, // catalog name
				"flink2hive", // default database
				"/etc/hive/conf", // Hive config (hive-site.xml) directory
				"1.2.1" // Hive version（现在只是支持1.2.1和2.3.4两个版本）
		);
		
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
		
		tableEnv.registerCatalog(category_name, catalog);
		tableEnv.useCatalog(category_name);
		
		// 创建Kafka表（本质是consumer），用户流计算
		tableEnv.useDatabase("stream_tmp");
		// 设置方言为默认（使用默认方言创建kafka相关表）
		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		String table_name = "analytics_access_log_kafka";
		String createKafkaLogTbl = " CREATE TABLE analytics_access_log_kafka (\n" + "   ts BIGINT,\n"
				+ "   userId BIGINT,\n" + "   eventType STRING,\n"
				+ "   siteId BIGINT,\n" + "   procTime AS PROCTIME(),\n"
				+ "   eventTime AS TO_TIMESTAMP(FROM_UNIXTIME(ts / 1000,'yyyy-MM-dd HH:mm:ss')),\n"
				+ "   WATERMARK FOR eventTime AS eventTime - INTERVAL '15' SECOND\n" + " ) WITH (\n"
				+ "   'connector' = 'kafka',\n" + "   'topic' = 'ods_analytics_access_log',\n"
				+ "   'properties.bootstrap.servers' = '172.16.103.96:9092',\n"
				+ "   'properties.zookeeper.connect' = '172.16.103.90:2181',\n"
				+ "   'properties.group.id' = 'flink_hive_integration_exp_1',\n"
				+ "   'scan.startup.mode' = 'latest-offset',\n" + "   'format' = 'json',\n"
				+ "   'json.fail-on-missing-field' = 'false',\n" + "   'json.ignore-parse-errors' = 'true'\n" + " )";
		
		ifNotExistCreateTable(tableEnv, table_name, createKafkaLogTbl);
		
		// 创建hive表，用于流批一体中流计算同步到Hive中
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		table_name = "analytics_access_count";
		tableEnv.useDatabase("flink2hive");
		// 创建Hivetable，用来记录指定时间段内各个url的访问数量(从Flink中同步到此表）
		String createCountTbl = "CREATE TABLE flink2hive.analytics_access_count (\n" + 
				"    url STRING,\n" + 
				"    access_count BIGINT,\n" + 
				"    start_time TIMESTAMP,\n" + 
				"    end_time TIMESTAMP\n" + 
				") STORED AS PARQUET;\n";
		ifNotExistCreateTable(tableEnv, table_name, createCountTbl);

		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		String insertSql = "INSERT into flink2hive.analytics_access_count\n" + 
				"    SELECT CAST(siteId as VARCHAR) as siteId\n" + 
				"    ,count(*) as cnt\n" + 
				"    ,TUMBLE_START(eventTime, INTERVAL '10' SECOND) as wStar\n" + 
				"    ,TUMBLE_END(eventTime, INTERVAL '10' SECOND) as wEnd\n" + 
				"    from stream_tmp.analytics_access_log_kafka\n" + 
				"    group by TUMBLE(eventTime, INTERVAL '10' SECOND), siteId";
		
		tableEnv.executeSql(insertSql);
	}
	
	private static boolean ifNotExistCreateTable(StreamTableEnvironment tableEnv, String table_name, String createSql) {
		boolean create_flag = false;
		if(!exists_check(tableEnv,table_name)) {
			tableEnv.executeSql(createSql);
			
			create_flag = true;
		}
		
		return create_flag;
	}

	/**
	 * 如果表不存在则创建，Hive Dialect并不支持IF NOT EXISTS语句

	 * @param tableEnv
	 * @param table_name
	 * @return
	 */
	private static boolean exists_check(StreamTableEnvironment tableEnv, String table_name) {
		String[] lst = tableEnv.listTables();// ("CREATE DATABASE IF NOT EXISTS stream_tmp")
		boolean exists_flag = false;
		for (String tblName : lst) {
			if (tblName.equals(table_name)) {
				exists_flag = true;
				break;
			}
		}

		return exists_flag;
	}
}
