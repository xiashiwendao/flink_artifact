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
		String default_DB= "flink2hive";
		HiveCatalog catalog = new HiveCatalog(category_name, // catalog name
				default_DB, // default database
				"/etc/hive/conf", // Hive config (hive-site.xml) directory
				"1.2.1" // Hive version（现在只是支持1.2.1和2.3.4两个版本）
		);
		
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE);
		tableEnv.getConfig().getConfiguration().set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(20));
		
		tableEnv.registerCatalog(category_name, catalog);
		tableEnv.useCatalog(category_name);
		
		// 创建Kafka表（本质是consumer），用户流计算
		tableEnv.useDatabase(default_DB);
		// 设置方言为默认（使用默认方言创建kafka相关表）
		tableEnv.getConfig().setSqlDialect(SqlDialect.DEFAULT);
		String table_name_kafka = "analytics_access_log_kafka_2";
		String createKafkaLogTbl = "CREATE TABLE " + table_name_kafka +" (\n" + 
				"   logTime STRING,\n" + 
				"   message STRING,\n" + 
				"   eventType STRING,\n" + 
				"   userId STRING,\n" + 
				"   siteId STRING,\n" + 
				"   procTime AS PROCTIME(),\n" + 
				"   eventTimestamp AS TO_TIMESTAMP(logTime),\n" + 
				"   WATERMARK FOR eventTimestamp AS eventTimestamp - INTERVAL '15' SECOND\n" + 
				" ) WITH (\n" + 
				"   'connector' = 'kafka',\n" + 
				"   'topic' = 'test_2',\n" + 
				"   'properties.bootstrap.servers' = 'cdh6:9092',\n" + 
				"   'properties.group.id' = 'apache_log_flink',\n" + 
				"   'scan.startup.mode' = 'latest-offset',\n" + 
				"   'format' = 'json',\n" + 
				"   'json.fail-on-missing-field' = 'false',\n" + 
				"   'json.ignore-parse-errors' = 'false'\n" + 
				" )\n" + 
				"";
		
		ifNotExistCreateTable(tableEnv, table_name_kafka, createKafkaLogTbl);
		
		// 创建hive表，用于流批一体中流计算同步到Hive中
		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		String table_name_hive = "analytics_access_count_2";
		tableEnv.useDatabase("flink2hive");
		// 创建Hivetable，用来记录指定时间段内各个url的访问数量(从Flink中同步到此表）
		String createCountTbl = "CREATE TABLE " + table_name_hive + " (\n" + 
				"    url STRING,\n" + 
				"    access_count BIGINT,\n" + 
				"    start_time TIMESTAMP,\n" + 
				"    end_time TIMESTAMP\n" + 
				") STORED AS PARQUET\n";
		ifNotExistCreateTable(tableEnv, table_name_hive, createCountTbl);

		tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);
		String insertSql = "INSERT into " + table_name_hive + " \n" + 
				"SELECT siteId\n" + 
				",count(*) as cnt\n" + 
				",TUMBLE_START(eventTimestamp, INTERVAL '20' SECOND) as wStar\n" + 
				",TUMBLE_END(eventTimestamp, INTERVAL '20' SECOND) as wEnd\n" + 
				"from " + table_name_kafka + "\n" + 
				"group by TUMBLE(eventTimestamp, INTERVAL '20' SECOND), siteId\n";
		
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
