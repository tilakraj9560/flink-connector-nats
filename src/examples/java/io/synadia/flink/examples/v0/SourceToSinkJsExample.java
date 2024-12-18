package io.synadia.flink.examples.v0;

import io.nats.client.*;
import io.nats.client.api.AckPolicy;
import io.nats.client.api.ConsumerConfiguration;
import io.nats.client.api.StreamConfiguration;
import io.synadia.flink.utils.PropertiesUtils;
import io.synadia.flink.v0.payload.StringPayloadDeserializer;
import io.synadia.flink.v0.payload.StringPayloadSerializer;
import io.synadia.flink.v0.sink.NatsSink;
import io.synadia.flink.v0.sink.NatsSinkBuilder;
import io.synadia.flink.v0.source.JetStreamSource;
import io.synadia.flink.v0.source.JetStreamSourceBuilder;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class SourceToSinkJsExample {
    public static void main(String[] args) throws Exception {
        // Load configuration from application.properties
        Properties props = PropertiesUtils.loadPropertiesFromFile("src/examples/resources/application.properties");

        // Define static names loaded from properties
        String sourceSubject = props.getProperty("source.JsSubject");
        String sinkSubject = props.getProperty("sink.JsSubject");
        String streamName = props.getProperty("source.stream");
        String consumerName = props.getProperty("source.consumer");

        // Connect to NATS server
        Connection nc = connect(props);
        JetStreamManagement jsm = nc.jetStreamManagement();
        JetStream js = nc.jetStream();


        // Create a JetStream stream for the source subject
        createStream(jsm, streamName, sourceSubject, "test-subject");

        // Create a consumer for the source subject on the stream
        createConsumer(jsm, streamName, sourceSubject, consumerName);

        // create a consumer for the test subject on the stream
        createConsumer(jsm, streamName, "test-subject", "test-consumer");

        // Publish test messages to the source subject
        publish(js, sourceSubject);

        // Publish test messages to the test subject
        publish(js, "test-subject");

        // List to capture sink messages received via the NATS dispatcher
        final List<Message> syncList = Collections.synchronizedList(new ArrayList<>());

        // Configure the NATS JetStream Source
        Properties connectionProperties = props;

        StringPayloadDeserializer deserializer = new StringPayloadDeserializer();

        Map<String, Set<String>> subjectConsumerGroups = new HashMap<>();

        subjectConsumerGroups.put(sourceSubject, Collections.singleton(consumerName));
        subjectConsumerGroups.put("test-subject", Collections.singleton("test-consumer"));

        JetStreamSourceBuilder<String> builder = new JetStreamSourceBuilder<String>()
                .subjects(sourceSubject)
                .enableAutoAck(false)
                .subjectConsumerGroups(subjectConsumerGroups)
                .payloadDeserializer(deserializer) // Deserialize messages from source
                .connectionProperties(connectionProperties);

        JetStreamSource<String> natsSource = builder.build();

        // Configure Flink environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getCheckpointConfig().setCheckpointInterval(10_000L); // Set checkpoint interval
        DataStream<String> ds = env.fromSource(natsSource, WatermarkStrategy.noWatermarks(), "nats-source-input");

        // Create a NATS dispatcher to listen to sink messages
        Dispatcher dispatcher = nc.createDispatcher();
        dispatcher.subscribe(sinkSubject, syncList::add); // Collect sink messages

        // Configure the NATS JetStream Sink
        NatsSink<String> sink = new NatsSinkBuilder<String>()
                .subjects(sinkSubject)
                .connectionProperties(connectionProperties)
                .payloadSerializer(new StringPayloadSerializer()) // Serialize messages for sink
                .build();

        // Sink the source messages to the NATS JetStream sink
        ds.sinkTo(sink);

        // Configure Flink restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5, Time.seconds(5)));

        // Execute Flink pipeline asynchronously
        env.execute("JetStream Source-to-Sink Example");

        // Allow the job to run for 20 seconds
        //Thread.sleep(60_000);

        // Gracefully close the dispatcher and Flink environment
        dispatcher.unsubscribe(sinkSubject);
        env.close();

        System.out.println("count : " + syncList.size());

        // Print received sink messages
        for (Message m : syncList) {
            String payload = new String(m.getData());
            System.out.println("Received message at sink: " + payload);
        }

        // Delete the stream after the test
        jsm.deleteStream(streamName);
        System.out.println("Stream deleted: " + streamName);

        // Close the NATS connection
        nc.close();

        // Terminate the application
        System.exit(0);
    }

    /**
     * Connect to the NATS server using provided properties.
     */
    private static Connection connect(Properties props) throws Exception {
        return Nats.connect(props.getProperty("io.nats.client.url"));
    }

    /**
     * Create a JetStream stream with the specified name and subject.
     */
    private static void createStream(JetStreamManagement jsm, String streamName, String...subjects) throws Exception {
        StreamConfiguration streamConfig = StreamConfiguration.builder()
                .name(streamName)
                .subjects(subjects)
                .build();
        jsm.addStream(streamConfig);
        System.out.println("Stream created: " + streamName);
    }

    /**
     * Create a durable consumer for the given stream and subject.
     */
    private static void createConsumer(JetStreamManagement jsm, String streamName, String subject, String consumerName) throws Exception {
        ConsumerConfiguration consumerConfig = ConsumerConfiguration.builder()
                .durable(consumerName) // Durable consumer for persistence
                .ackPolicy(AckPolicy.All) // Explicit acknowledgement policy
                .filterSubject(subject) // Filter messages for this subject
                .deliverPolicy(io.nats.client.api.DeliverPolicy.New) // Deliver new messages
                .build();
        jsm.addOrUpdateConsumer(streamName, consumerConfig);
        System.out.println("Consumer created: " + consumerName);
    }

    /**
     * Publish a fixed number of test messages to the specified JetStream subject.
     */
    private static void publish(JetStream js, String subject) throws Exception {
        for (int i = 0; i < 100; i++) {
            String msg = "Message " + i + " for " + subject;
            js.publish(subject, msg.getBytes());
        }
    }
}
