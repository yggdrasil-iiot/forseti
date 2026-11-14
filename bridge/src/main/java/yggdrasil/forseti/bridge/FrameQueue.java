package yggdrasil.forseti.bridge;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/** The bounded hand-over from the MQTT callback thread to the one thread that drives the {@link Pipeline}. A frame that finds the
 *  queue still full after {@code offerTimeoutMs} fails the run instead of being dropped, and so does any failure the MQTT side reports
 *  (a lost connection: a reconnect would hide the frames lost meanwhile). */
public final class FrameQueue {
    private final BlockingQueue<Frame> queue;
    private final long offerTimeoutMs;
    private final AtomicReference<BridgeFailure> failure = new AtomicReference<>();

    public FrameQueue(int capacity, long offerTimeoutMs) {
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.offerTimeoutMs = offerTimeoutMs;
    }

    /** Called on the MQTT thread. */
    public void offer(Frame f) {
        try {
            if (!queue.offer(f, offerTimeoutMs, TimeUnit.MILLISECONDS))
                fail(new BridgeFailure("the input queue stayed full for " + offerTimeoutMs + " ms at " + f.topic()));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            fail(new BridgeFailure("interrupted while queueing " + f.topic(), e));
        }
    }

    public void fail(BridgeFailure f) { failure.compareAndSet(null, f); }

    /** The next frame, or null after {@code timeoutMs}; throws the first failure reported. */
    public Frame poll(long timeoutMs) throws InterruptedException {
        BridgeFailure f = failure.get();
        if (f != null) throw f;
        return queue.poll(timeoutMs, TimeUnit.MILLISECONDS);
    }

    /** The frames already queued, for the drain at the end of a run; throws the first failure reported. */
    public Frame pollNow() {
        BridgeFailure f = failure.get();
        if (f != null) throw f;
        return queue.poll();
    }
}
