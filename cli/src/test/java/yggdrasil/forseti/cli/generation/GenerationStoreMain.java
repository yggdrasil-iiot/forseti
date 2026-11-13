package yggdrasil.forseti.cli.generation;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.generation.Generation;
import yggdrasil.forseti.core.generation.GenerationPolicy;

/** Forked by {@link FileGenerationStoreTest}: waits for the go file, then allocates the next number and appends its {@code registered}
 *  row once, inside one hold of {@code generations.lock}, and prints the number. */
public final class GenerationStoreMain {
    private GenerationStoreMain() {}

    static final String RULES = "a".repeat(64);
    static final long AT = 1794182460000L;

    /** A row of {@code n} with fixed pins; {@code run} and {@code gateReportSha256} as the status requires. */
    static Generation row(long n, String status) {
        boolean withRun = List.of(Generation.REPLAYING, Generation.VERIFIED, Generation.CURRENT).contains(status);
        boolean withReport = List.of(Generation.VERIFIED, Generation.CURRENT).contains(status);
        JsonNode run = null;
        if (withRun) {
            ObjectNode r = Json.object().put("runId", "00000000-0000-4000-8000-000000000001").put("runRecordSha256", "b".repeat(64));
            r.putObject("offsetRanges");
            run = r;
        }
        ObjectNode evidence = Json.object().put("releaseSha256", "d".repeat(64));
        return new Generation(n, status, RULES, Json.JSON.createArrayNode(), Json.object(), 1, evidence, withReport ? "c".repeat(64) : null, run, AT);
    }

    /** Allocates and appends the next {@code registered} row of {@code scope} inside one lock hold; returns its number. */
    static long register(FileGenerationStore store, String scope) {
        return store.locked(() -> {
            long n = GenerationPolicy.next(store.highest());
            store.append(scope, row(n, Generation.REGISTERED));
            return n;
        });
    }

    public static void main(String[] args) throws Exception {
        Path state = Path.of(args[0]), go = Path.of(args[2]);
        long deadline = System.currentTimeMillis() + 30_000;
        while (!Files.exists(go)) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("no go file");
            Thread.onSpinWait();
        }
        System.out.println(register(new FileGenerationStore(state), args[1]));
    }
}
