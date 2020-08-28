package kafkatoflink;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
* @Description    KafkaProducerTest 发送Kafka消息
* @Author         0262000099 Hengtai Nie
* @CreateDate     2018/9/21 11:29
*/
public class KafkaProducerTest {
  public static void main(String[] args) throws Exception {
    Properties props = new Properties();
    props.put("bootstrap.servers", "172.16.103.96:9092");
    props.put("acks", "all");
    props.put("retries", 0);
    props.put("batch.size", 16384);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 33554432);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

    Producer<String, String> producer = new KafkaProducer<>(props);
    int totalMessageCount = 100;
    for (int i = 0; i < totalMessageCount; i++) {
      String value = String.format("%d,%s,%d", System.currentTimeMillis(), "machine-1", currentMemSize());
      producer.send(new ProducerRecord<>("test-0921", value), new Callback() {
        @Override
        public void onCompletion(RecordMetadata metadata, Exception exception) {
          if (exception != null) {
            System.out.println("Failed to send message with exception " + exception);
          }
        }
      });
      Thread.sleep(100L);
    }
    producer.close();
    System.out.println("complete produce data!");
  }

  private static long currentMemSize() {
    return 1000;
  }
}
