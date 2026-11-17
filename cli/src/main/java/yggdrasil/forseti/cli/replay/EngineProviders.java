package yggdrasil.forseti.cli.replay;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

/** The engine path and the {@link java.util.ServiceLoader} lookups of the launcher (plan D1): the {@link EngineProvider}s of
 *  {@code gate|run|requirements --engine <name>} and the transport store providers of {@code generation --store <name>}. The engine path
 *  is the system property {@value #PATH_PROPERTY}, else the environment variable {@value #PATH_ENV}, else the directory {@code engines}
 *  beside the running jar; it lists entries separated by the platform's path separator, each a jar, a classes directory, or a directory
 *  whose jars are all taken. Providers are loaded in a class loader of their own over that path (its parent the launcher's), so the core
 *  jar itself never carries an engine or a transport client: an engine the path does not hold is simply not found. The thread context
 *  class loader is that loader during discovery and during every call into a provider ({@link Loaded#call}), and is restored after it:
 *  libraries that load classes by name through it (Kafka's client configuration does) then see the engine path. */
public final class EngineProviders {
    private EngineProviders() {}

    public static final String PATH_PROPERTY = "forseti.engine.path", PATH_ENV = "FORSETI_ENGINE_PATH", DEFAULT_DIR = "engines", CORE = "core";

    /** A provider and the loader it came from. */
    public record Loaded<S>(S provider, ClassLoader loader) {
        /** {@code body} with the thread context class loader set to {@link #loader}, restored afterwards whatever happens. */
        public <T> T call(Supplier<T> body) { return withLoader(loader, body); }
    }

    /** {@code body} with {@code loader} as the thread context class loader, the previous one restored in {@code finally}. */
    public static <T> T withLoader(ClassLoader loader, Supplier<T> body) {
        Thread t = Thread.currentThread();
        ClassLoader previous = t.getContextClassLoader();
        t.setContextClassLoader(loader);
        try { return body.get(); } finally { t.setContextClassLoader(previous); }
    }

    /** The engine path of this process. */
    public static List<Path> enginePath() {
        String p = System.getProperty(PATH_PROPERTY);
        if (p == null || p.isEmpty()) p = System.getenv(PATH_ENV);
        if (p != null && !p.isEmpty()) return parse(p);
        Path beside = besideJar();
        return beside != null && Files.isDirectory(beside) ? List.of(beside) : List.of();
    }

    static List<Path> parse(String path) {
        List<Path> l = new ArrayList<>();
        for (String e : path.split(java.util.regex.Pattern.quote(File.pathSeparator))) if (!e.isEmpty()) l.add(Path.of(e));
        return List.copyOf(l);
    }

    private static Path besideJar() {
        try {
            Path jar = Path.of(EngineProviders.class.getProtectionDomain().getCodeSource().getLocation().toURI());
            return Files.isRegularFile(jar) ? jar.resolveSibling(DEFAULT_DIR) : null;
        } catch (Exception e) { return null; }
    }

    /** The class path the engine path stands for, in order. */
    static List<URL> urls(List<Path> path) {
        List<URL> urls = new ArrayList<>();
        try {
            for (Path e : path) {
                if (Files.isRegularFile(e)) { urls.add(e.toUri().toURL()); continue; }
                if (!Files.isDirectory(e)) continue;
                urls.add(e.toUri().toURL());
                try (Stream<Path> s = Files.list(e)) {
                    for (Path j : s.filter(x -> x.getFileName().toString().endsWith(".jar") && Files.isRegularFile(x)).sorted().toList()) urls.add(j.toUri().toURL());
                }
            }
        } catch (MalformedURLException e) { throw new IllegalArgumentException(e); }
        catch (IOException e) { throw new UncheckedIOException(e); }
        return urls;
    }

    /** The provider of {@code service} named {@code name} ({@code nameOf}) on {@code path}; empty when none is. Two providers of one
     *  name is an {@link IllegalStateException}. */
    public static <S> Optional<Loaded<S>> load(Class<S> service, String name, Function<S, String> nameOf, List<Path> path) {
        ClassLoader loader = new URLClassLoader("forseti-engines", urls(path).toArray(URL[]::new), EngineProviders.class.getClassLoader());
        S found = withLoader(loader, () -> {
            S f = null;
            for (S p : ServiceLoader.load(service, loader)) {
                if (!name.equals(nameOf.apply(p))) continue;
                if (f != null) throw new IllegalStateException("two " + service.getSimpleName() + "s are named " + name + ": " + f.getClass().getName() + " and " + p.getClass().getName());
                f = p;
            }
            return f;
        });
        return found == null ? Optional.empty() : Optional.of(new Loaded<>(found, loader));
    }

    /** The {@link EngineProvider} named {@code name} on {@code path}. {@code core} is never looked up. */
    public static Optional<Loaded<EngineProvider>> find(String name, List<Path> path) {
        if (CORE.equals(name)) throw new IllegalArgumentException("--engine core runs in-process, it is not a provider");
        return load(EngineProvider.class, name, EngineProvider::name, path);
    }

    /** {@code gate}, {@code run} or {@code requirements} of a loaded provider, inside its loader. */
    public static int dispatch(Loaded<EngineProvider> p, String[] args, PrintStream out, PrintStream err) {
        return p.call(() -> switch (args[0]) {
            case "gate" -> p.provider().gate(args, out, err);
            case "run" -> p.provider().run(args, out, err);
            case "requirements" -> p.provider().requirements(args, out, err);
            default -> throw new IllegalArgumentException("not an engine command: " + args[0]);
        });
    }

    /** The value of {@code --engine} in {@code args} from index {@code from}, if given. */
    public static Optional<String> engineOf(String[] args, int from) {
        for (int i = from; i + 1 < args.length; i++) if (args[i].equals("--engine")) return Optional.of(args[i + 1]);
        return Optional.empty();
    }
}
