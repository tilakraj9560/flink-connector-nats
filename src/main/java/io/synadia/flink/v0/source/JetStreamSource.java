// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details.

package io.synadia.flink.v0.source;

import io.nats.client.JetStream;
import io.synadia.flink.utils.ConnectionFactory;
import io.synadia.flink.v0.enumerator.JetStreamSourceEnumerator;
import io.synadia.flink.v0.payload.PayloadDeserializer;
import io.synadia.flink.v0.source.reader.JetStreamSourceReader;
import io.synadia.flink.v0.source.split.JetStreamSplit;
import io.synadia.flink.v0.source.split.JetStreamSplitCheckpointSerializer;
import io.synadia.flink.v0.source.split.JetStreamSplitSerializer;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.*;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;


import static io.synadia.flink.utils.MiscUtils.generateId;

public class JetStreamSource<T> implements Source<T, JetStreamSplit, Collection<JetStreamSplit>>,
        ResultTypeQueryable<T>
{
    protected final String id;
    protected final List<JetStreamSplit> splits;
    protected final PayloadDeserializer<T> payloadDeserializer;
    protected final ConnectionFactory connectionFactory;
    protected final Logger logger;
    protected final JetStreamSourceConfiguration sourceConfiguration;

    public JetStreamSource(PayloadDeserializer<T> payloadDeserializer, ConnectionFactory connectionFactory, JetStreamSourceConfiguration sourceConfiguration)
    {
        this(payloadDeserializer, connectionFactory, sourceConfiguration, JetStreamSource.class);
    }

    protected JetStreamSource(PayloadDeserializer<T> payloadDeserializer, ConnectionFactory connectionFactory, JetStreamSourceConfiguration sourceConfiguration, Class<?> logClazz)
    {

        id = generateId();

        this.payloadDeserializer = payloadDeserializer;
        this.connectionFactory = connectionFactory;
        this.sourceConfiguration = sourceConfiguration;
        logger = LoggerFactory.getLogger(logClazz);

        this.splits = getSplits(sourceConfiguration);
    }

    private List<JetStreamSplit> getSplits(JetStreamSourceConfiguration sourceConfiguration) {
        // create splits from subject consumer groups
        List<JetStreamSplit> streamSplits = new ArrayList<>();
        sourceConfiguration.subjectConsumerGroups.forEach((subject, consumerGroups) -> {
            consumerGroups.forEach(consumerGroup -> {
                streamSplits.add(new JetStreamSplit(subject, consumerGroup));
            });
        });

        return streamSplits;
    }

    @Override
    public Boundedness getBoundedness() {
        logger.debug("{} | Boundedness", id);
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SplitEnumerator<JetStreamSplit, Collection<JetStreamSplit>> createEnumerator(
            SplitEnumeratorContext<JetStreamSplit> enumContext) throws Exception
    {
        logger.debug("{} | createEnumerator", id);
        return restoreEnumerator(enumContext, splits);
    }

    @Override
    public SplitEnumerator<JetStreamSplit, Collection<JetStreamSplit>> restoreEnumerator(SplitEnumeratorContext<JetStreamSplit> splitEnumeratorContext, Collection<JetStreamSplit> jetStreamSplits) throws Exception {
        logger.debug("{} | restoreEnumerator", id);
        return new JetStreamSourceEnumerator(splitEnumeratorContext, jetStreamSplits);
    }

    @Override
    public SimpleVersionedSerializer<JetStreamSplit> getSplitSerializer() {
        logger.debug("{} | getSplitSerializer", id);
        return new JetStreamSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<Collection<JetStreamSplit>> getEnumeratorCheckpointSerializer() {
        logger.debug("{} | getEnumeratorCheckpointSerializer", id);
        return new JetStreamSplitCheckpointSerializer();
    }

    @Override
    public SourceReader<T, JetStreamSplit> createReader(SourceReaderContext readerContext) throws Exception {
        logger.debug("{} | createReader", id);

        JetStream jetStream = connectionFactory.connect().jetStream();
        return new JetStreamSourceReader<>(jetStream, splits, payloadDeserializer);
    }

    @Override
    public TypeInformation<T> getProducedType() {
        return payloadDeserializer.getProducedType();
    }

    @Override
    public String toString() {
        return "NatsSource{" +
                "id='" + id + '\'' +
                ", splits=" + splits +
                ", payloadDeserializer=" + payloadDeserializer.getClass().getCanonicalName() +
                ", connectionFactory=" + connectionFactory +
                '}';
    }
}
