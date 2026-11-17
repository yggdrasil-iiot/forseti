package yggdrasil.forseti.cli.replay;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import yggdrasil.forseti.cli.Main;
import yggdrasil.forseti.cli.generation.GenerationCli;
import yggdrasil.forseti.cli.generation.GenerationStoreProvider;
import yggdrasil.forseti.cli.generation.OperatorStore;

/** The launcher's lookups (plan D1): {@code --engine core} stays in-process, any other engine is the {@link EngineProvider} of that name
 *  that {@link java.util.ServiceLoader} finds on the engine path, which receives the whole command with the engine loader as the thread
 *  context class loader; no provider of that name is a usage error, exit 2. {@code generation --store <name>} resolves its store the
 *  same way. */
class EngineProvidersTest {
    public static final class FakeProvider implements EngineProvider {
        static final List<String> CALLS = new ArrayList<>();
        static final List<ClassLoader> LOADERS = new ArrayList<>();
        public FakeProvider() { LOADERS.add(Thread.currentThread().getContextClassLoader()); }
        @Override public String name() { return "fake"; }
        @Override public int gate(String[] args, PrintStream out, PrintStream err) { seen("gate", args); out.println("fake gate"); return 0; }
        @Override public int run(String[] args, PrintStream out, PrintStream err) { seen("run", args); return 1; }
        @Override public int requirements(String[] args, PrintStream out, PrintStream err) { seen("requirements", args); err.println("not implemented until step 6 PR 7"); return 2; }
        static void seen(String what, String[] args) { CALLS.add(what + " " + String.join(" ", args)); LOADERS.add(Thread.currentThread().getContextClassLoader()); }
    }

    public static final class OtherFake implements EngineProvider {
        @Override public String name() { return "fake"; }
        @Override public int gate(String[] args, PrintStream out, PrintStream err) { return 0; }
        @Override public int run(String[] args, PrintStream out, PrintStream err) { return 0; }
        @Override public int requirements(String[] args, PrintStream out, PrintStream err) { return 0; }
    }

    /** A store provider that only proves it was found and called inside the engine loader. */
    public static final class FakeStoreProvider implements GenerationStoreProvider {
        static final List<String> OPENED = new ArrayList<>();
        @Override public String name() { return "fake-store"; }
        @Override public OperatorStore open(Map<String, String> options) {
            OPENED.add(options + " " + Thread.currentThread().getContextClassLoader().getName());
            throw new IllegalArgumentException("fake-store opened");
        }
    }

    @TempDir Path tmp;

    @AfterEach void clear() { System.clearProperty(EngineProviders.PATH_PROPERTY); FakeProvider.CALLS.clear(); FakeProvider.LOADERS.clear(); FakeStoreProvider.OPENED.clear(); }

    Path enginePath(String... providers) throws IOException {
        Path dir = tmp.resolve("engines");
        Path f = dir.resolve("META-INF/services/" + EngineProvider.class.getName());
        Files.createDirectories(f.getParent());
        Files.writeString(f, String.join("\n", providers) + "\n", StandardCharsets.UTF_8);
        Files.writeString(dir.resolve("META-INF/services/" + GenerationStoreProvider.class.getName()), FakeStoreProvider.class.getName() + "\n", StandardCharsets.UTF_8);
        return dir;
    }

    @Test void aProviderIsFoundByNameOnTheEnginePathOnly() throws IOException {
        Path dir = enginePath(FakeProvider.class.getName());
        assertTrue(EngineProviders.find("fake", List.of(dir)).isPresent());
        assertTrue(EngineProviders.find("flink", List.of(dir)).isEmpty());
        assertTrue(EngineProviders.find("fake", List.of()).isEmpty(), "the launcher's own class path holds no provider");
        assertThrows(IllegalArgumentException.class, () -> EngineProviders.find("core", List.of(dir)));
        assertEquals(List.of(Path.of("a"), Path.of("b")), EngineProviders.parse("a" + java.io.File.pathSeparator + java.io.File.pathSeparator + "b"));
    }

    @Test void twoProvidersOfOneNameAreRefused() throws IOException {
        Path dir = enginePath(FakeProvider.class.getName());
        Files.createDirectories(tmp.resolve("second/META-INF/services"));
        Files.writeString(tmp.resolve("second/META-INF/services/" + EngineProvider.class.getName()), OtherFake.class.getName() + "\n");
        assertThrows(IllegalStateException.class, () -> EngineProviders.find("fake", List.of(dir, tmp.resolve("second"))));
    }

    @Test void discoveryAndEveryCallRunInTheEngineLoaderWhichIsRestoredAfter() throws IOException {
        ClassLoader before = Thread.currentThread().getContextClassLoader();
        EngineProviders.Loaded<EngineProvider> p = EngineProviders.find("fake", List.of(enginePath(FakeProvider.class.getName()))).orElseThrow();
        assertEquals(before, Thread.currentThread().getContextClassLoader(), "restored after discovery");
        assertEquals(p.loader(), FakeProvider.LOADERS.get(0), "the provider is instantiated inside the engine loader");
        assertEquals("forseti-engines", p.loader().getName());
        PrintStream sink = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertEquals(0, EngineProviders.dispatch(p, new String[] {"gate", "--engine", "fake"}, sink, sink));
        assertEquals(p.loader(), FakeProvider.LOADERS.get(1));
        assertEquals(before, Thread.currentThread().getContextClassLoader(), "restored after the call");
        assertThrows(IllegalStateException.class, () -> p.call(() -> { throw new IllegalStateException("x"); }));
        assertEquals(before, Thread.currentThread().getContextClassLoader(), "restored after a throw");
    }

    @Test void theLauncherHandsAnExternalEngineTheWholeCommand() throws IOException {
        System.setProperty(EngineProviders.PATH_PROPERTY, enginePath(FakeProvider.class.getName()).toString());
        ByteArrayOutputStream o = new ByteArrayOutputStream(), e = new ByteArrayOutputStream();
        PrintStream out = new PrintStream(o, true, StandardCharsets.UTF_8), err = new PrintStream(e, true, StandardCharsets.UTF_8);
        assertEquals(0, Main.run(new String[] {"gate", "--engine", "fake", "--out", "build/fake"}, out, err));
        assertEquals(1, Main.run(new String[] {"run", "--engine", "fake", "--its-own-switch"}, out, err));
        assertEquals(2, Main.run(new String[] {"requirements", "--engine", "fake", "--gate-report", "r.json"}, out, err));
        assertEquals(List.of("gate gate --engine fake --out build/fake", "run run --engine fake --its-own-switch", "requirements requirements --engine fake --gate-report r.json"),
            FakeProvider.CALLS);
        assertTrue(o.toString(StandardCharsets.UTF_8).contains("fake gate"));
        for (ClassLoader l : FakeProvider.LOADERS) assertEquals("forseti-engines", l.getName());
    }

    @Test void aMissingProviderIsAUsageErrorAndCoreHasNoRequirementsRun() {
        System.setProperty(EngineProviders.PATH_PROPERTY, tmp.resolve("nothing").toString());
        ByteArrayOutputStream e = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(e, true, StandardCharsets.UTF_8), out = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        assertEquals(2, Main.run(new String[] {"gate", "--engine", "flink"}, out, err));
        assertEquals(2, Main.run(new String[] {"run", "--engine", "flink", "--recording", "x"}, out, err));
        assertEquals(2, Main.run(new String[] {"requirements", "--engine", "flink"}, out, err));
        assertTrue(e.toString(StandardCharsets.UTF_8).contains("--engine flink: no engine provider of that name on the engine path"), e.toString(StandardCharsets.UTF_8));
        assertEquals(2, Main.run(new String[] {"requirements", "--engine", "core"}, out, err));
        assertTrue(e.toString(StandardCharsets.UTF_8).contains("--engine core has no requirements run"), e.toString(StandardCharsets.UTF_8));
        assertTrue(FakeProvider.CALLS.isEmpty());
    }

    @Test void aTransportStoreIsResolvedOnTheEnginePathOrIsAUsageError() throws IOException {
        System.setProperty(EngineProviders.PATH_PROPERTY, tmp.resolve("nothing").toString());
        ByteArrayOutputStream e = new ByteArrayOutputStream();
        PrintStream err = new PrintStream(e, true, StandardCharsets.UTF_8), out = new PrintStream(new ByteArrayOutputStream(), true, StandardCharsets.UTF_8);
        String[] list = {"list", "--state", tmp.resolve("state").toString(), "--store", "fake-store", "--bootstrap", "localhost:1", "--generations-topic", "t"};
        assertEquals(2, GenerationCli.run(list, out, err, java.time.Clock.systemUTC()));
        assertTrue(e.toString(StandardCharsets.UTF_8).contains("--store fake-store: no store provider of that name"), e.toString(StandardCharsets.UTF_8));
        assertEquals(2, GenerationCli.run(new String[] {"list", "--state", tmp.toString(), "--bootstrap", "localhost:1"}, out, err, java.time.Clock.systemUTC()),
            "transport flags with the file store");
        System.setProperty(EngineProviders.PATH_PROPERTY, enginePath(FakeProvider.class.getName()).toString());
        assertEquals(2, GenerationCli.run(list, out, err, java.time.Clock.systemUTC()));
        assertEquals(List.of("{bootstrap=localhost:1, mode=reader, topic=t} forseti-engines"), FakeStoreProvider.OPENED);
    }
}
