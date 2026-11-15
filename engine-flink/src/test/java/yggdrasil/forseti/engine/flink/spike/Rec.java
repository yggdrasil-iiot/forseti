package yggdrasil.forseti.engine.flink.spike;

/** One record of a spike source: the split it came from, its position in that split and its event timestamp. A Flink POJO. */
public class Rec {
  public int split;
  public int seq;
  public long ts;

  public Rec() {}

  Rec(int split, int seq, long ts) {
    this.split = split;
    this.seq = seq;
    this.ts = ts;
  }
}
