import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

public class ReadCommittedSource {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.19.150.232:9092");
        props.setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        final FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("test", new SimpleStringSchema(), props);
        consumer.setStartFromLatest();

        env.addSource(consumer).map(v -> System.currentTimeMillis() + " " + v).print();

        env.execute("ReadCommittedSource");
    }
}

/*
在Kafka0.11集群上运行正常。
在Kafka0.10集群上运行报错：[Cannot create a v1 ListOffsetRequest because we require features supported only in 2 or later.]
00:37:47,173 WARN  org.apache.flink.streaming.connectors.kafka.internal.KafkaFetcher  - Error while closing Kafka consumer
java.lang.NullPointerException
	at org.apache.flink.streaming.connectors.kafka.internal.KafkaConsumerThread.run(KafkaConsumerThread.java:282)
00:37:47,175 INFO  org.apache.flink.runtime.taskmanager.Task                     - Source: Custom Source -> Map -> Sink: Print to Std. Out (1/1) (07d9ac74f08e13931d27b934dd8b3a4b) switched from RUNNING to FAILED.
org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v1 ListOffsetRequest because we require features supported only in 2 or later.
00:37:47,176 INFO  org.apache.flink.runtime.taskmanager.Task                     - Freeing task resources for Source: Custom Source -> Map -> Sink: Print to Std. Out (1/1) (07d9ac74f08e13931d27b934dd8b3a4b).
00:37:47,176 INFO  org.apache.flink.runtime.taskmanager.Task                     - Ensuring all FileSystem streams are closed for task Source: Custom Source -> Map -> Sink: Print to Std. Out (1/1) (07d9ac74f08e13931d27b934dd8b3a4b) [FAILED]
00:37:47,176 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor            - Un-registering task and sending final execution state FAILED to JobManager for task Source: Custom Source -> Map -> Sink: Print to Std. Out (1/1) 07d9ac74f08e13931d27b934dd8b3a4b.
00:37:47,177 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> Map -> Sink: Print to Std. Out (1/1) (07d9ac74f08e13931d27b934dd8b3a4b) switched from RUNNING to FAILED on org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@15daa052.
org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v1 ListOffsetRequest because we require features supported only in 2 or later.
 */