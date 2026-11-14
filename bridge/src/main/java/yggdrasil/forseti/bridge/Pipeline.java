package yggdrasil.forseti.bridge;

import java.util.ArrayList;
import java.util.List;

/** Core, then the declared D3 stages in order, then the writer. One thread drives it, so the frames of an edge keep their arrival order
 *  end to end; the stages see only the core's output and never feed back into the session tracker. */
public final class Pipeline {
    private final BridgeCore core;
    private final List<Stage> stages;
    private final Sink sink;
    private final Records.Topics topics;
    private final RunRecord run;
    private final RawFrameLog log;

    /** @param log the raw-frame log, or null when the run keeps none */
    public Pipeline(BridgeCore core, List<Stage> stages, Sink sink, Records.Topics topics, RunRecord run, RawFrameLog log) {
        this.core = core;
        this.stages = List.copyOf(stages);
        this.sink = sink;
        this.topics = topics;
        this.run = run;
        this.log = log;
    }

    public List<Stage> stages() { return stages; }
    public RunRecord run() { return run; }
    public BridgeCore core() { return core; }

    public void accept(Frame f, long now) {
        run.frame();
        if (log != null) log.append(f);
        through(0, core.accept(f), now);
    }

    /** Lets time pass: a hold window that has ended releases its records. */
    public void tick(long now) {
        for (int i = 0; i < stages.size(); i++) through(i + 1, stages.get(i).tick(now), now);
    }

    /** End of the run: every held record is released and written, then every write is acknowledged. */
    public void drain(long now) {
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
        }
    }
}
