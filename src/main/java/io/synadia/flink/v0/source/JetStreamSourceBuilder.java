// Copyright (c) 2023-2024 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.synadia.flink.v0.NatsSinkOrSourceBuilder;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import java.time.Duration;
import java.util.Map;
import java.util.Set;

import static io.synadia.flink.utils.Constants.*;

public class JetStreamSourceBuilder<T> extends NatsSinkOrSourceBuilder<T, JetStreamSourceBuilder<T>> {

    protected PayloadDeserializer<T> payloadDeserializer;
    protected String payloadDeserializerClass;
    protected int messageQueueCapacity;
    protected boolean enableAutoAcknowledgeMessage;
    protected Duration fetchOneMessageTimeout;
    protected Duration fetchTimeout;
    protected int maxFetchRecords;
    protected Duration autoAckInterval;

    // each unique subject with its consumer groups
    protected Map<String, Set<String>> subjectConsumerGroups;

    public JetStreamSourceBuilder() {
        super(SOURCE_PREFIX);

        messageQueueCapacity = DEFAULT_ELEMENT_QUEUE_CAPACITY;
        enableAutoAcknowledgeMessage = DEFAULT_ENABLE_AUTO_ACK;
        fetchOneMessageTimeout = Duration.ofMillis(DEFAULT_FETCH_ONE_MESSAGE_TIMEOUT_MS);
        fetchTimeout = Duration.ofMillis(DEFAULT_FETCH_TIMEOUT_MS);
        maxFetchRecords = DEFAULT_MAX_FETCH_RECORDS;
        autoAckInterval = Duration.ofMillis(DEFAULT_AUTO_ACK_INTERVAL_MS);
    }

    /**
     * @param enableAutoAck the enableAutoAck to set
     * @return the builder
     */
    public JetStreamSourceBuilder<T> enableAutoAck (boolean enableAutoAck) {
        this.enableAutoAcknowledgeMessage = enableAutoAck;
        return this;
    }

    /**
     * @param subjectConsumerGroups the subjectConsumerGroups to set
     * @return the builder
     */
    public JetStreamSourceBuilder<T> subjectConsumerGroups (Map<String, Set<String>> subjectConsumerGroups) {
        this.subjectConsumerGroups = subjectConsumerGroups;
        return this;
    }

    /**
     * Set the payload deserializer for the source.
     * @param payloadDeserializer the deserializer.
     * @return the builder
     */
    public JetStreamSourceBuilder<T> payloadDeserializer(PayloadDeserializer<T> payloadDeserializer) {
        this.payloadDeserializer = payloadDeserializer;
        this.payloadDeserializerClass = null;
        return this;
    }

    /**
     * Set the fully qualified name of the desired class payload deserializer for the source.
     * @param payloadDeserializerClass the serializer class name.
     * @return the builder
     */
    public JetStreamSourceBuilder<T> payloadDeserializerClass(String payloadDeserializerClass) {
        this.payloadDeserializer = null;
        this.payloadDeserializerClass = payloadDeserializerClass;
        return this;
    }

    /**
     * Build a NatsJetStreamSource.
     * @return the source
     */
    public JetStreamSource<T> build() {

        if (payloadDeserializer == null) {
            if (payloadDeserializerClass == null) {
                throw new IllegalStateException("Valid payload deserializer class must be provided.");
            }

            // so much can go wrong here... ClassNotFoundException, ClassCastException
            try {
                //noinspection unchecked
                payloadDeserializer = (PayloadDeserializer<T>) Class.forName(payloadDeserializerClass).getDeclaredConstructor().newInstance();
            }
            catch (Exception e) {
                throw new IllegalStateException("Valid payload serializer class must be provided.", e);
            }
        }

        if (subjectConsumerGroups == null || subjectConsumerGroups.isEmpty()) {
            throw new IllegalStateException("At least one subject with consumer groups must be provided.");
        }

        baseBuild();

        return new JetStreamSource<>(payloadDeserializer, createConnectionFactory(), new JetStreamSourceConfiguration(
                subjectConsumerGroups,
                messageQueueCapacity,
                enableAutoAcknowledgeMessage,
                fetchOneMessageTimeout,
                fetchTimeout,
                maxFetchRecords,
                autoAckInterval));
    }

    @Override
    protected JetStreamSourceBuilder<T> getThis() {
        return this;
    }
}