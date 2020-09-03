import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class StreamDemo1 {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        String bootstrapServers = "kafka.local:9092";
        String sourceTopic = "hello-kafka-input";
        String sinkTopic = "hello-kafka-output";
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "yahoo");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        Serde<String> stringSerde = Serdes.String();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> simpleFirstStream = builder.stream(sourceTopic, Consumed.with(stringSerde, stringSerde));
        KStream<String, String> upperCasedStream = simpleFirstStream.mapValues( rec -> rec.toLowerCase());
        upperCasedStream.to(sinkTopic, Produced.with(stringSerde, stringSerde));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);
        kafkaStreams.start();
        Thread.sleep(60000);
        kafkaStreams.close();
    }
}
