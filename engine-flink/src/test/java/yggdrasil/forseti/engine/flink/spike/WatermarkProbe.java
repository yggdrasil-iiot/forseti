package yggdrasil.forseti.engine.flink.spike;

import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.runtime.state.VoidNamespaceSerializer;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.InternalTimerService;
import org.apache.flink.streaming.api.operators.InternalTimerServiceImpl;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A keyed operator that records, into {@link Probe}, its state initialization (whether it was restored and from which checkpoint, and
 * the execution attempt), the operator watermark of its event-time timer service at every record, every watermark it processes, and the
 * event-time timers it holds after initialization. With {@code timerAt > 0} the first record of attempt 0 registers one event-time
 * timer at {@code timerAt}, so a restore can show that timers come back while the watermark does not.
 */
final class WatermarkProbe extends AbstractStreamOperator<Void> implements OneInputStreamOperator<Rec, Void>, Triggerable<Integer, VoidNamespace> {
  private final String name;
  private final long timerAt;
  private transient InternalTimerService<VoidNamespace> timers;
  private transient boolean timerRegistered;

  WatermarkProbe(String name, long timerAt) {
    this.name = name;
    this.timerAt = timerAt;
  }

  private int attempt() {
    return getContainingTask().getEnvironment().getTaskInfo().getAttemptNumber();
  }

  @Override
  public void initializeState(StateInitializationContext context) throws Exception {
    super.initializeState(context);
    Probe.add(name, new Probe.Event("INIT", attempt(), 0, -1, -1, 0, context.isRestored(),
        context.getRestoredCheckpointId().orElse(-1L)));
  }

  @Override
  public void open() throws Exception {
    super.open();
    timers = getInternalTimerService("probe", VoidNamespaceSerializer.INSTANCE, this);
    int held = ((InternalTimerServiceImpl<?, ?>) timers).numEventTimeTimers();
    Probe.add(name, new Probe.Event("TIMERS", attempt(), 0, -1, -1, held, false, -1));
  }

  @Override
  public void processElement(StreamRecord<Rec> element) throws Exception {
    Rec r = element.getValue();
    Probe.add(name, new Probe.Event("ELEM", attempt(), 0, r.split, r.seq, timers.currentWatermark(), false, -1));
    if (timerAt > 0 && !timerRegistered && attempt() == 0) {
      timers.registerEventTimeTimer(VoidNamespace.INSTANCE, timerAt);
      timerRegistered = true;
    }
  }

  @Override
  public void processWatermark(Watermark mark) throws Exception {
    super.processWatermark(mark);
    Probe.add(name, new Probe.Event("WM", attempt(), 0, -1, -1, mark.getTimestamp(), false, -1));
  }

  @Override
  public void onEventTime(InternalTimer<Integer, VoidNamespace> timer) {}

  @Override
  public void onProcessingTime(InternalTimer<Integer, VoidNamespace> timer) {}
}
