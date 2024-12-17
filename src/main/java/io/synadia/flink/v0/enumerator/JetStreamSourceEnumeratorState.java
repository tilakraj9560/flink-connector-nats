// Copyright (c) 2023 Synadia Communications Inc. All Rights Reserved.
// See LICENSE and NOTICE file for details. 

package io.synadia.flink.v0.enumerator;

import io.synadia.flink.v0.source.split.JetStreamSplit;
import org.apache.flink.annotation.Internal;

import java.util.Set;

/** The state of Nats source enumerator. */
@Internal
public class JetStreamSourceEnumeratorState {
    private final Set<JetStreamSplit> unassignedSplits;

    public JetStreamSourceEnumeratorState(Set<JetStreamSplit> unassignedSplits) {
        this.unassignedSplits = unassignedSplits;
    }

    public Set<JetStreamSplit> getUnassignedSplits() {
        return unassignedSplits;
    }
}
