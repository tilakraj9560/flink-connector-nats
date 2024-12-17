/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, MiscUtils 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.synadia.flink.v0.source.split;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;

import java.io.IOException;

/**
 * Serializes and deserializes the {@link JetStreamSplit}. This class needs to handle
 * deserializing splits from older versions.
 */
@Internal
public class JetStreamSplitSerializer implements SimpleVersionedSerializer<JetStreamSplit> {

    public static final int CURRENT_VERSION = 1;

    @Override
    public int getVersion() {
        return CURRENT_VERSION;
    }

    @Override
    public byte[] serialize(JetStreamSplit split) throws IOException {
        final DataOutputSerializer out =
            new DataOutputSerializer(split.splitId().length());
        serializeV1(out, split);
        return out.getCopyOfBuffer();
    }

    public static void serializeV1(DataOutputView out, JetStreamSplit split) throws IOException {
        out.writeUTF(split.splitId());
    }

    @Override
    public JetStreamSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != CURRENT_VERSION) {
            throw new IOException("Unrecognized version: " + version);
        }
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        return deserializeV1(in);
    }

    static JetStreamSplit deserializeV1(DataInputView in) throws IOException {
        String input = in.readUTF();
        String[] elements = input.split("_");


        if (elements.length != 2) {
            throw new IOException("Invalid split format: " + input);
        }

        String subject = elements[0];
        String consumerName = elements[1];


        return new JetStreamSplit(subject, consumerName);
    }
}
