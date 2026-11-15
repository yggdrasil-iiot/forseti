package yggdrasil.forseti.bridge;

import java.util.List;
import java.util.Map;

/** A declared, default-off stage in front of the bridge core: it sees the MQTT frames after the raw-frame log has kept them in wire
 *  order and before the session tracker classifies them, so it may change the order in which the core sees them (and nothing else:
 *  a frame is passed on unchanged, never dropped). Every input stage that runs logs itself in the run record. */
public interface InputStage {
    /** The frames to hand to the core, in order, for one frame received; may be empty while the stage holds it. */
    List<Frame> apply(Frame f);
    /** Everything still held, at the end of the run: an input stage never drops a frame. */
    List<Frame> drain();
    /** The stage's declaration and what it did, for the run record. */
    Map<String, Object> record();
}
