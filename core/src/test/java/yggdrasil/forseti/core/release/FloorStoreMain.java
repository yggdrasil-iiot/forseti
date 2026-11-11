package yggdrasil.forseti.core.release;

import java.nio.file.Path;

/** Forked by {@link FloorStoreTest}: waits for the go file, then raises the floor of one stream once and prints the outcome. */
public final class FloorStoreMain {
    private FloorStoreMain() {}
    public static void main(String[] args) throws Exception {
        Path state = Path.of(args[0]), go = Path.of(args[4]);
        long deadline = System.currentTimeMillis() + 30_000;
        while (!java.nio.file.Files.exists(go)) {
            if (System.currentTimeMillis() > deadline) throw new IllegalStateException("no go file");
            Thread.onSpinWait();
        }
        FloorStore.Outcome o = new FloorStore(state).update(args[1], Long.parseLong(args[2]), args[3], true);
        System.out.println(o.getClass().getSimpleName());
    }
}
