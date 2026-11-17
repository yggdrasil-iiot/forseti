package yggdrasil.forseti.engine.flink.job;

import yggdrasil.forseti.cli.replay.OutputRecords;

/**
 * One output record of a writer, ready for its transactional sink: the run's topic name, the destination partition chosen explicitly
 * (the station's partition {@code p(s)}, or 6 for a reject collector), and the transport form of the step-3 journal line
 * ({@link OutputRecords#transport}): RFC 8785 key and value bytes and the {@code forseti-generation} header, the only header any output
 * record carries. A Flink POJO.
 */
public final class Out {
  public String topic;
  public int partition;
  public byte[] key;
  public byte[] value;
  public String generationHeader;

  public Out() {}

  public static Out of(String topic, int partition, OutputRecords.Transport t) {
    if (t.headers().size() != 1 || !t.headers().containsKey(OutputRecords.GENERATION_HEADER))
      throw new IllegalArgumentException("an output record carries exactly the header " + OutputRecords.GENERATION_HEADER + ": " + t.headers().keySet());
    OutputRecords.requireCanonical(t);
    Out o = new Out();
    o.topic = topic;
    o.partition = partition;
    o.key = t.key();
    o.value = t.value();
    o.generationHeader = t.headers().get(OutputRecords.GENERATION_HEADER);
    return o;
  }
}
