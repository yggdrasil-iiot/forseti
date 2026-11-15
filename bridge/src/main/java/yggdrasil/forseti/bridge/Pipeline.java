package yggdrasil.forseti.bridge;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/** The raw-frame log, the declared input stages, the core, then the declared D3 stages in order, then the writer. One thread drives
 *  it, so the frames of an edge keep their arrival order end to end unless a declared input stage permutes them (injection 25(a)); the
 *  raw-frame log always keeps the wire order. The D3 stages see only the core's output and never feed back into the session tracker. */
public final class Pipeline {
    private final BridgeCore core;
    private final List<InputStage> inputs;
    private final List<Stage> stages;
    private final Sink sink;
    private final Records.Topics topics;
    private final RunRecord run;
    private final RawFrameLog log;
    private final Consumer<Emitted.SessionEvent> sessions;

    /** @param log the raw-frame log, or null when the run keeps none */
    public Pipeline(BridgeCore core, List<Stage> stages, Sink sink, Records.Topics topics, RunRecord run, RawFrameLog log) {
        this(core, stages, sink, topics, run, log, null);
    }

    /** @param sessions told of every session event once it is handed to the writer (the process prints it), or null */
    public Pipeline(BridgeCore core, List<Stage> stages, Sink sink, Records.Topics topics, RunRecord run, RawFrameLog log, Consumer<Emitted.SessionEvent> sessions) {
        this(core, List.of(), stages, sink, topics, run, log, sessions);
    }

    /** @param inputs the declared input stages, in order, between the raw-frame log and the core */
    public Pipeline(BridgeCore core, List<InputStage> inputs, List<Stage> stages, Sink sink, Records.Topics topics, RunRecord run, RawFrameLog log,
                    Consumer<Emitted.SessionEvent> sessions) {
        this.sessions = sessions;
        this.core = core;
        this.inputs = List.copyOf(inputs);
        this.stages = List.copyOf(stages);
        this.sink = sink;
        this.topics = topics;
        this.run = run;
        this.log = log;
    }

    public List<InputStage> inputs() { return inputs; }
    public List<Stage> stages() { return stages; }
    public RunRecord run() { return run; }
    public BridgeCore core() { return core; }

    public void accept(Frame f, long now) {
        run.frame();
        if (log != null) log.append(f);
        frames(0, List.of(f), now);
    }

    /** The frames through the input stages from {@code from} on, then each through the core and the D3 stages. */
    private void frames(int from, List<Frame> frames, long now) {
        List<Frame> cur = frames;
        for (int i = from; i < inputs.size(); i++) {
            List<Frame> next = new ArrayList<>();
            for (Frame x : cur) next.addAll(inputs.get(i).apply(x));
            cur = next;
        }
        for (Frame x : cur) through(0, core.accept(x), now);
    }

    /** Lets time pass: a hold window that has ended releases its records. */
    public void tick(long now) {
        for (int i = 0; i < stages.size(); i++) through(i + 1, stages.get(i).tick(now), now);
    }

    /** End of the run: every held frame and record is released and written, then every write is acknowledged. */
    public void drain(long now) {
        for (int i = 0; i < inputs.size(); i++) frames(i + 1, inputs.get(i).drain(), now);
        for (int i = 0; i < stages.size(); i++) through(i + 1, stages.get(i).drain(now), now);
        sink.flush();
    }

    private void through(int from, List<Emitted> records, long now) {
        List<Emitted> cur = records;
        for (int i = from; i < stages.size(); i++) {
            List<Emitted> next = new ArrayList<>();
            for (Emitted e : cur) next.addAll(stages.get(i).apply(e, now));
            cur = next;
        }
        for (Emitted e : cur) {
            Records.Out r = Records.encode(e, topics);
            sink.send(r);
            run.written(r);
            if (sessions != null && e instanceof Emitted.SessionEvent se) sessions.accept(se);
        }
    }
}
