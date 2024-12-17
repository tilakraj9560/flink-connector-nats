package io.synadia.flink.v0.source.split;

import lombok.Getter;
import org.apache.flink.api.connector.source.SourceSplit;
import java.io.Serializable;


@Getter
public class JetStreamSplit implements SourceSplit, Serializable {

    // A unique identifier for the serialized version of this class
    private static final long serialVersionUID = 1L;

    private final String subject;  // The JetStream subject
    private final String consumerName; // The consumer name
    private final long lastSeq;  // Last processed sequence

    // Constructor with default lastSeq (-1)
    public JetStreamSplit(String subject, String consumerName) {
        this.subject = subject;
        this.consumerName = consumerName;
        this.lastSeq = -1; // Hold garbage value
    }

    // Constructor with lastSeq
    public JetStreamSplit(String subject, String consumerName, long lastSeq) {
        this.subject = subject;
        this.consumerName = consumerName;
        this.lastSeq = lastSeq;
    }

    @Override
    public String splitId() {
        return subject + "_" + consumerName;  // Unique ID per split
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        JetStreamSplit that = (JetStreamSplit) o;

        return subject.equals(that.subject) && consumerName.equals(that.consumerName);
    }

    @Override
    public int hashCode() {
        int result = subject.hashCode();
        result = 31 * result + consumerName.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "JetStreamSplit{" +
                "subject='" + subject + '\'' +
                ", consumerName='" + consumerName + '\'' +
                ", lastSeq=" + lastSeq +  // Output lastSeq value
                '}';
    }
}