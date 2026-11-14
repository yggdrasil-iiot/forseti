package yggdrasil.forseti.loop;

import com.fasterxml.jackson.databind.node.ObjectNode;

/** Where the driver sends the occupancy and incident events it builds. {@link InputProducer} writes them to Kafka; the tests collect
 *  them. A send returns only once the record is acknowledged, so a failed delivery stops the drive instead of leaving a gap. */
public interface EventSink {
    void occupancy(ObjectNode event);
    void incident(ObjectNode revision);
}
