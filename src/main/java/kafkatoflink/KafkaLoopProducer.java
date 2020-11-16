package kafkatoflink;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import com.alibaba.fastjson.JSON;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.joda.time.DateTime;

/**
 * 往kafka中写数据，可以使用这个main函数进行测试一下
 */
public class KafkaLoopProducer {
//    static class Metric {
//        public String name;
//        public long timestamp;
//        public Map<String, Object> fields;
//        public Map<String, String> tags;
//        public Metric(){}
//    }
    
    public static final String broker_list = "172.16.103.96:9092";
    public static final String topic = "metric";  // kafka topic，Flink 程序中需要和这个统一 
    private static Long count = 0L;
    public static void writeToKafka() throws InterruptedException {
    	count += 1;
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Metric metric = new Metric();
        metric.timestamp=System.currentTimeMillis();
        metric.name="mem";
        if(count%2 == 0)
        	metric.url = "www.baidu.com";
        else
        	metric.url = "leecode.org";
        Map<String, String> tags = new HashMap<>();
        Map<String, Object> fields = new HashMap<>();
        tags.put("cluster", "zhisheng");
        tags.put("host_ip", "101.147.022.106");

        fields.put("used_percent", 90d);
        fields.put("max", 27244873d);
        fields.put("used", 17244873d);
        fields.put("init", 900);

        metric.tags=tags;
        metric.fields=fields;

        ProducerRecord record = new ProducerRecord<String, String>(topic, null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }
    
    public static void writeToKafka2() throws InterruptedException {
    	count += 1;
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Access_Log metric = new Access_Log();
        long sec = new Date().getTime();
        metric.columnType = "columnType: " + sec;
        metric.eventType = "eventType: " + sec;
        metric.fromType = "fromType: " + sec;
        metric.grouponId = sec;
        metric.merchandiseId = sec;
        metric.partnerId = sec;
        Random r = new Random();
        metric.siteId = r.nextInt(3);
        metric.ts = sec;
        metric.userId = sec;
        
        ProducerRecord record = new ProducerRecord<String, String>("ods_analytics_access_log", null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }
    
    public static void writeToKafka3() throws InterruptedException {
    	count += 1;
        Properties props = new Properties();
        props.put("bootstrap.servers", broker_list);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列化
        KafkaProducer producer = new KafkaProducer<String, String>(props);

        Apache_Log metric = new Apache_Log();
        long sec = new Date().getTime();
        metric.message = "message: " + sec;
        metric.referrer = "refferer: " + sec;
        metric.response = "200";
        metric.tags = "tags";
        metric.verb = "GET";
        
        ProducerRecord record = new ProducerRecord<String, String>("test-2", null, null, JSON.toJSONString(metric));
        producer.send(record);
        System.out.println("发送数据: " + JSON.toJSONString(metric));

        producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        int count = 1500;
        
        while (count > 0) {
            Thread.sleep(2000);
            writeToKafka3();

            count--;
        }
    }
}