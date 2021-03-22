import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class ExactlyOnceTransactionSink {
    public static void main(String[] args) throws Exception {
        final LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        env.enableCheckpointing(1000 * 60);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.19.150.232:9092");
        //配置事务的超时时间小于Kafka消息的超时时间，但大于检查点的生成间隔时间
        props.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "120000");
        final FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
                "test", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), props,
                //必须为KafkaProducer指定ExactlyOnce，默认是AtLeastOnce
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        env.addSource(new SourceFunction<String>() {
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                long counter = 0;
                while (true) {
                    System.out.println(counter);
                    sourceContext.collect(String.valueOf(counter++));
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {

            }
        })
                .addSink(producer);

        env.execute("test");
    }
}

/*
在Kafka0.11集群上运行正常。
在Kafka0.10集群上运行报错：[Cannot create a v0 FindCoordinator request because we require features supported only in 1 or later.]
00:40:52,044 INFO  org.apache.flink.runtime.taskmanager.Task                     - Source: Custom Source -> Sink: Unnamed (1/1) (a813ac1202bf1ba22034678f7c8aafc7) switched from RUNNING to FAILED.
org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v0 FindCoordinator request because we require features supported only in 1 or later.
00:40:52,044 INFO  org.apache.flink.runtime.taskmanager.Task                     - Freeing task resources for Source: Custom Source -> Sink: Unnamed (1/1) (a813ac1202bf1ba22034678f7c8aafc7).
00:40:52,045 INFO  org.apache.flink.runtime.taskmanager.Task                     - Ensuring all FileSystem streams are closed for task Source: Custom Source -> Sink: Unnamed (1/1) (a813ac1202bf1ba22034678f7c8aafc7) [FAILED]
00:40:52,046 INFO  org.apache.flink.runtime.taskexecutor.TaskExecutor            - Un-registering task and sending final execution state FAILED to JobManager for task Source: Custom Source -> Sink: Unnamed (1/1) a813ac1202bf1ba22034678f7c8aafc7.
00:40:52,048 INFO  org.apache.flink.runtime.executiongraph.ExecutionGraph        - Source: Custom Source -> Sink: Unnamed (1/1) (a813ac1202bf1ba22034678f7c8aafc7) switched from RUNNING to FAILED on org.apache.flink.runtime.jobmaster.slotpool.SingleLogicalSlot@1d6364ed.
org.apache.kafka.common.errors.UnsupportedVersionException: Cannot create a v0 FindCoordinator request because we require features supported only in 1 or later.
 */