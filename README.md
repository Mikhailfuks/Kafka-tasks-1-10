tasks 1 
import logging
from kafka import KafkaProducer

# Настройка логгера
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Настройка обработчика для отправки сообщений в Kafka
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
kafka_handler = logging.StreamHandler(producer)
kafka_handler.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
kafka_handler.setFormatter(formatter)
logger.addHandler(kafka_handler)

# Отправка сообщения
logger.info("Hello from Kafka Logger!")

   tasks 2 

from kafka import KafkaProducer, KafkaConsumer

# Производитель
producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
producer.send('queue-topic', b'Message 1')
producer.send('queue-topic', b'Message 2')
producer.flush()

# Потребитель
consumer = KafkaConsumer('queue-topic', bootstrap_servers=['localhost:9092'],
                        group_id='my-group', auto_offset_reset='earliest', consumer_timeout_ms=1000)

for message in consumer:
    print(f'Message: {message.value.decode("utf-8")}')
    consumer.commit() # Сохранить смещение

tasks 3 


import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

public class KafkaFlinkWordCount {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Kafka Source
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-consumer");
        DataStream<String> stream = env.addSource(new FlinkKafkaConsumer<>("flink-topic", new SimpleStringSchema(), props));

        // Word count
        DataStream<Tuple2<String, Integer>> wordCounts = stream.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split("\s+");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        })
        .keyBy(0)
        .sum(1);

        // Kafka Sink
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("group.id", "flink-producer");
        wordCounts.addSink(new FlinkKafkaProducer<>("flink-wordcount-topic", new SimpleStringSchema(), props));

        env.execute("Kafka Flink WordCount");
    }
}

tasks 4 
from kafka import KafkaProducer
from kafka.errors import KafkaError
import ssl

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                        security_protocol='SSL',
                        ssl_context=ssl.create_default_context(purpose=ssl.Purpose.SERVER_AUTH),
                        ssl_cafile='/path/to/ca.pem',  # Сертификат CA
                        ssl_certfile='/path/to/client.cert.pem', # Сертификат клиента
                        ssl_keyfile='/path/to/client.key.pem')  # Ключ клиента

producer.send('my-topic', b'Hello, Secure Kafka!')
producer.flush()
 {

tasks 5
  "name": "cassandra-source",
  "connector.class": "io.confluent.connect.cassandra.CassandraSourceConnector",
  "tasks.max": "1",
  "connection.host": "localhost",
  "connection.port": "9042",
  "keyspace": "mykeyspace",
  "table": "mytable",
  "topic.prefix": "cassandra-",
  "mode": "incrementing",
  "incrementing.column.name": "id"
}
tasks 6 
 "name": "kafka-s3-sink",
  "connector.class": "io.confluent.connect.s3.S3SinkConnector",
  "tasks.max": "1",
  "topic": "my-topic",
  "s3.region": "us-east-1",
  "s3.bucket.name": "my-bucket",
  "s3.path.prefix": "data/",
  "s3.access.key": "your_access_key",
  "s3.secret.key": "your_secret_key",
  "key.converter": "org.apache.kafka.connect.json.JsonConverter",
  "value.converter": "org.apache.kafka.connect.json.JsonConverter"
}

tasks 7

rom kafka import KafkaAdminClient

admin_client = KafkaAdminClient(bootstrap_servers=['localhost:9092'])
admin_client.create_topics([{'topic': 'my-topic'}])



from kafka import KafkaConsumer

consumer = KafkaConsumer('my-topic', bootstrap_servers=['localhost:9092'],
                        group_id='my-group', enable_auto_commit=True)

for message in consumer:
    print(f'Message: {message.value.decode("utf-8")}')
tasks 8 

from kafka import KafkaConsumer

    consumer = KafkaConsumer('my-topic', bootstrap_servers=['localhost:9092'],
                            group_id='my-group', enable_auto_commit=False)
    
    for message in consumer:
        print(f'Message: {message.value.decode("utf-8")}')
        consumer.commit()  # Сохранить смещение вручную

    tasks 9 

    from kafka import KafkaProducer, KafkaConsumer

        # Производитель
        producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
        producer.send('keyed-topic', key=b'key1', value=b'value1')
        producer.send('keyed-topic', key=b'key2', value=b'value2')
        producer.flush()
        
        # Консьюмер
        consumer = KafkaConsumer('keyed-topic', bootstrap_servers=['localhost:9092'])
        consumer.assign([TopicPartition('keyed-topic', 0)])  # Выбирается один раздел
        for message in consumer:
            if message.key == b'key1':
                print(f'Key: {message.key.decode("utf-8")}, Value: {message.value.decode("utf-8")}')

tasks 10

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;

public class KafkaStreamsExample {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "my-stream-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> stream = builder.stream("stream-topic");
        stream.mapValues(value -> "Processed: " + value)
              .to("processed-topic");

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
















































































   
