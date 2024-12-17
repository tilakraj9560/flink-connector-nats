package io.synadia.flink.v0.source.reader;

import io.nats.client.*;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.split.JetStreamSplit;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.IOException;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;

public class JetStreamSourceReader<T> implements SourceReader<T, JetStreamSplit> {
    private final JetStream jetStream;  // JetStream instance
    private final List<JetStreamSplit> splits;  // Active splits (topics/partitions)
    private final Map<String, Long> lastProcessedMessageIds = new ConcurrentHashMap<>();  // Keeps track of the last message processed for each split
    private final Map<String, JetStreamSubscription> subscriptions = new ConcurrentHashMap<>();
    private final PayloadDeserializer<T> payloadDeserializer;
    private final ThreadPoolExecutor executor;

    public JetStreamSourceReader(JetStream jetStream, List<JetStreamSplit> splits, PayloadDeserializer<T> payloadDeserializer) {
        this.jetStream = jetStream;
        this.splits = splits;
        this.payloadDeserializer = payloadDeserializer;

        this.executor = new ThreadPoolExecutor(
                1,  2, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>() // task queue
        );
    }

    @Override
    public void start() {
        for (JetStreamSplit split : splits) {
            String subject = split.getSubject();
            String consumerName = split.getConsumerName();

            // Subscribe to the subject
            try {
                PullSubscribeOptions options = PullSubscribeOptions.builder()
                        .durable(consumerName)
                        .build();

                JetStreamSubscription subscription = jetStream.subscribe(subject, options);
                subscriptions.put(split.splitId(), subscription);
            } catch (IOException | JetStreamApiException e) {

                throw new FlinkRuntimeException("Failed to create pull subscription", e);
            }
        }
    }

    @Override
    public InputStatus pollNext(ReaderOutput<T> output) throws Exception {
        List<CompletableFuture<Void>> completableFutures = new ArrayList<>();
        List<String> subscriptionsToRemove = new CopyOnWriteArrayList<>();

        // Poll messages from each active subscription
        for (Map.Entry<String, JetStreamSubscription> entry : subscriptions.entrySet()) {
            String splitId = entry.getKey();
            JetStreamSubscription subscription = entry.getValue();

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    // Fetch the next message (blocking until the message is available)
                    List<Message> messages = subscription.fetch(10, Duration.ofSeconds(50));  // Fetch 1 message (pull-based)


                    // if messages are empty, remove the subscription
                    if (messages == null || messages.isEmpty()) {
                        subscriptionsToRemove.remove(splitId);
                        return;
                    }

                    for (Message message : messages) {
                        // Acknowledge the message to indicate successful processing
                        //message.ackSync(Duration.ofSeconds(10));
                        message.ack();

                        // Process the message
                        output.collect(payloadDeserializer.getObject(message.getSubject(), message.getData(), message.getHeaders()));

                        // Update the state with the last processed stream seq
                        lastProcessedMessageIds.put(splitId, message.metaData().streamSequence());
                    }

                    System.out.println("Reading from subscription ended: " + subscription.getConsumerName());
                } catch (Exception e) {

                    System.out.println("Exception while processing message: " + e.getMessage());
                    throw new FlinkRuntimeException("Exception while processing message", e);
                }
            }, executor);

            completableFutures.add(future);
        }

        CompletableFuture<Void> allOf = CompletableFuture.allOf(completableFutures.toArray(new CompletableFuture[0]));
        allOf.join();

        for (CompletableFuture<Void> future : completableFutures) {
            try {
                future.get();
            } catch (Exception e) {
                // Handle exception if any future failed
                throw new FlinkRuntimeException("Exception occurred during polling", e);
            }
        }

        if (completableFutures.stream().anyMatch(CompletableFuture::isCompletedExceptionally)) {
            return InputStatus.MORE_AVAILABLE;
        }

        for (String splitId : subscriptionsToRemove) {
            subscriptions.get(splitId).unsubscribe();
            subscriptions.remove(splitId);
        }

        if (subscriptions.isEmpty()) {
            return InputStatus.END_OF_INPUT;
        }

        return InputStatus.MORE_AVAILABLE;
    }

    @Override
    public List<JetStreamSplit> snapshotState(long checkpointId) {
        // Save the state for each split
        List<JetStreamSplit> snapshot = new ArrayList<>();
        for (JetStreamSplit split : splits) {
            long lastSeq = lastProcessedMessageIds.getOrDefault(split.splitId(), -1L);
            if (lastSeq == -1) {
                snapshot.add(new JetStreamSplit(split.getSubject(), split.getConsumerName()));
                continue;
            }

            snapshot.add(new JetStreamSplit(split.getSubject(), split.getConsumerName(), lastSeq + 1));
        }

        return snapshot;
    }

    @Override
    public CompletableFuture<Void> isAvailable() {
        // Return immediately since the source is always available
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public void addSplits(List<JetStreamSplit> splits) {
        this.splits.addAll(splits);

        int minNoOfThreads = this.splits.size();
        if (minNoOfThreads > 2) {
            this.executor.setMaximumPoolSize(minNoOfThreads);
        }

        // Subscribe to the subjects corresponding to new splits
        start();
    }

    @Override
    public void notifyNoMoreSplits() {
        // Handle the case when no more splits are available
    }

    @Override
    public void close() throws Exception {
        // Clean up resources
        for (JetStreamSubscription subscription : subscriptions.values()) {
            subscription.unsubscribe();
        }

        subscriptions.clear();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        // Handle checkpoint completion if needed
    }
}