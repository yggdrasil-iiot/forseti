package yggdrasil.forseti.cli.replay;

import java.nio.file.Path;
import java.util.Map;

/** The journal dump of a replay run (plan D10): after every engine process has terminated, one {@code read_committed} consumer reads
 *  every output journal from its first offset to its end offset and writes it in the step-3 journal format
 *  ({@code <out>/journals/<topic>/<p>.jsonl}, {@code <out>/quarantine/<p>.jsonl}, one RFC 8785 line per record, {@link OutputRecords}),
 *  translated to recording offsets ({@link JournalTranslation}), so that the comparison, {@code JournalFold} and the oracle run
 *  unchanged on it. Returns the number of records dumped per {@code <topic>/<p>}. */
public interface JournalDump extends AutoCloseable {
    Map<String, Long> dump(Path out, OffsetMap offsets);

    @Override void close();
}
