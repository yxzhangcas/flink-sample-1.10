import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.Serdes;

import java.util.Collections;
import java.util.Properties;

public class ReadCommittedConsumer {
    public static void main(String[] args) {
        final Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.19.150.232:9092");
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        //配置“已提交读”，避免读取脏数据
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        final Consumer<String, String> consumer = new KafkaConsumer<String, String>(props, Serdes.String().deserializer(), Serdes.String().deserializer());
        consumer.subscribe(Collections.singletonList("test"));

        while (true) {
            final ConsumerRecords<String, String> records = consumer.poll(1000);
            //从时间戳信息和运行状态可以看出，确实是事务性读写，也证明了Flink是事务性写入
            for (ConsumerRecord<String, String> record : records) {
                System.out.println(System.currentTimeMillis() + " " + record.value());
            }
        }
    }
}

/*
在Kafka0.11集群上运行正常。
在Kafka0.10集群上运行报错：[Cannot create a v1 ListOffsetRequest because we require features supported only in 2 or later.]
Exception in thread "main" org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v1 ListOffsetRequest because we require features supported only in 2 or later.
 */