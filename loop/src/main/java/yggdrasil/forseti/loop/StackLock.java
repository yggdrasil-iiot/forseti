package yggdrasil.forseti.loop;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The lock of the full-loop stack ({@code live/stack-lock.json}): the two broker images by digest and the two upstream jars (the
 *  Bifrost sim and Muninn) by the commit they are built from and the SHA-256 of the jar that build produces. The jars are not committed;
 *  the runner builds them from the locked commits and refuses to start when a jar or an image is not the locked one, so a run names
 *  exactly the binaries it ran.
 *
 *  <pre>
 *  {
 *    "stackLockVersion": 1,
 *    "images": {"kafka": {"image": "apache/kafka", "version": "4.2.2", "digest": "sha256:..."}, ...},
 *    "jars": {"bifrost-sim": {"repository": "yggdrasil-iiot/bifrost", "commit": "<40 hex>", "module": "sim",
 *                             "path": "sim/target/bifrost-sim.jar", "sha256": "<64 hex>"}, ...}
 *  }
 *  </pre>
 *  {@code images} are keyed by compose service, {@code jars} by name; {@code module} is the Maven module to build ({@code "."} for the
 *  root). The compose file must reference every locked image as {@code <image>@<digest>} and no other image, never by tag. */
public final class StackLock {
    private StackLock() {}

    public record Image(String service, String image, String version, String digest) {
        public String reference() { return image + "@" + digest; }
    }

    public record Jar(String name, String repository, String commit, String module, String path, String sha256) {}

    public record Lock(SortedMap<String, Image> images, SortedMap<String, Jar> jars) {}

    private static final Pattern DIGEST = Pattern.compile("sha256:[0-9a-f]{64}"), SHA = Pattern.compile("[0-9a-f]{64}"),
        COMMIT = Pattern.compile("[0-9a-f]{40}"), NAME = Pattern.compile("[a-z0-9][a-z0-9._-]*"),
        IMAGE_LINE = Pattern.compile("^\\s*image:\\s*[\"']?([^\"'\\s#]+)[\"']?\\s*(#.*)?$");

    /** Reads and checks a lock; anything malformed is refused with the field named. */
    public static Lock read(JsonNode n) {
        if (!n.isObject()) throw new IllegalArgumentException("stack lock: a JSON object expected");
        fields(n, Set.of("stackLockVersion", "images", "jars"), "stack lock");
        if (Json.safeLong(n.get("stackLockVersion"), "stackLockVersion", 1, 1) != 1) throw new IllegalArgumentException("stack lock: stackLockVersion 1 expected");
        SortedMap<String, Image> images = new TreeMap<>();
        JsonNode is = n.path("images");
        if (!is.isObject() || is.isEmpty()) throw new IllegalArgumentException("stack lock: images is a non-empty object");
        for (Iterator<Map.Entry<String, JsonNode>> it = is.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String where = "stack lock: images." + e.getKey();
            name(e.getKey(), where);
            fields(e.getValue(), Set.of("image", "version", "digest"), where);
            String digest = Json.text(e.getValue(), "digest");
            if (!DIGEST.matcher(digest).matches()) throw new IllegalArgumentException(where + ".digest is not sha256:<64 hex>");
            String image = Json.text(e.getValue(), "image");
            if (image.contains("@") || image.contains(":")) throw new IllegalArgumentException(where + ".image names a tag or digest: " + image);
            images.put(e.getKey(), new Image(e.getKey(), image, Json.text(e.getValue(), "version"), digest));
        }
        SortedMap<String, Jar> jars = new TreeMap<>();
        JsonNode js = n.path("jars");
        if (!js.isObject() || js.isEmpty()) throw new IllegalArgumentException("stack lock: jars is a non-empty object");
        for (Iterator<Map.Entry<String, JsonNode>> it = js.fields(); it.hasNext(); ) {
            Map.Entry<String, JsonNode> e = it.next();
            String where = "stack lock: jars." + e.getKey();
            name(e.getKey(), where);
            fields(e.getValue(), Set.of("repository", "commit", "module", "path", "sha256"), where);
            String commit = Json.text(e.getValue(), "commit"), sha = Json.text(e.getValue(), "sha256");
            if (!COMMIT.matcher(commit).matches()) throw new IllegalArgumentException(where + ".commit is not 40 lowercase hex digits");
            if (!SHA.matcher(sha).matches()) throw new IllegalArgumentException(where + ".sha256 is not 64 lowercase hex digits");
            jars.put(e.getKey(), new Jar(e.getKey(), Json.text(e.getValue(), "repository"), commit, Json.text(e.getValue(), "module"),
                Json.text(e.getValue(), "path"), sha));
        }
        return new Lock(Collections.unmodifiableSortedMap(images), Collections.unmodifiableSortedMap(jars));
    }

    /** The image references of a compose file: the value of every {@code image:} line, in file order. */
    public static List<String> composeImages(String compose) {
        List<String> l = new ArrayList<>();
        for (String line : compose.split("\n", -1)) {
            Matcher m = IMAGE_LINE.matcher(line.replace("\r", ""));
            if (m.matches()) l.add(m.group(1));
        }
        return l;
    }

    /** Every way the compose file departs from the lock: a locked image it does not reference by its digest, an image it references
     *  that is not locked, and any reference by tag. */
    public static List<String> composeProblems(Lock lock, String compose) {
        List<String> p = new ArrayList<>();
        List<String> refs = composeImages(compose);
        Set<String> locked = new TreeSet<>();
        for (Image i : lock.images().values()) locked.add(i.reference());
        for (String r : refs) {
            if (!r.contains("@sha256:")) p.add("compose image " + r + " is not referenced by digest");
            else if (!locked.contains(r)) p.add("compose image " + r + " is not in the stack lock");
        }
        for (String r : locked) if (!refs.contains(r)) p.add("compose file does not reference the locked image " + r);
        return p;
    }

    /** Every jar given ({@code name -> file}) whose SHA-256 is not the lock's; a name the lock does not have, or a locked jar not
     *  given, is a problem too. */
    public static List<String> jarProblems(Lock lock, Map<String, Path> given) {
        List<String> p = new ArrayList<>();
        for (String n : given.keySet()) if (!lock.jars().containsKey(n)) p.add("jar " + n + " is not in the stack lock");
        for (Jar j : lock.jars().values()) {
            Path f = given.get(j.name());
            if (f == null) { p.add("jar " + j.name() + " was not given"); continue; }
            if (!Files.isRegularFile(f)) { p.add("jar " + j.name() + ": no file " + f); continue; }
            String sha = sha256(f);
            if (!sha.equals(j.sha256())) p.add("jar " + j.name() + " (" + f.getFileName() + ") has SHA-256 " + sha + ", the stack lock pins " + j.sha256()
                + " (built from " + j.repository() + " " + j.commit() + ")");
        }
        return p;
    }

    /** Every running service whose image is not the locked digest. {@code observed}: service -> the repo digests of the image its
     *  container runs ({@code docker inspect}'s {@code RepoDigests}, {@code <image>@sha256:...}); a locked service not given is a
     *  problem. */
    public static List<String> imageProblems(Lock lock, Map<String, List<String>> observed) {
        List<String> p = new ArrayList<>();
        for (String s : observed.keySet()) if (!lock.images().containsKey(s)) p.add("service " + s + " is not in the stack lock");
        for (Image i : lock.images().values()) {
            List<String> o = observed.get(i.service());
            if (o == null) { p.add("service " + i.service() + ": no image observed"); continue; }
            if (!o.contains(i.reference())) p.add("service " + i.service() + " runs " + o + ", not the locked " + i.reference());
        }
        return p;
    }

    static String sha256(Path f) {
        try { return Canonical.digestOf(Files.readAllBytes(f)); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    static String read(Path f) {
        try { return Files.readString(f, StandardCharsets.UTF_8); } catch (IOException e) { throw new UncheckedIOException(e); }
    }

    private static void name(String n, String where) {
        if (!NAME.matcher(n).matches()) throw new IllegalArgumentException(where + ": the name is not [a-z0-9][a-z0-9._-]*");
    }

    private static void fields(JsonNode n, Set<String> allowed, String where) {
        if (!n.isObject()) throw new IllegalArgumentException(where + ": an object expected");
        for (Iterator<String> it = n.fieldNames(); it.hasNext(); ) {
            String f = it.next();
            if (!allowed.contains(f)) throw new IllegalArgumentException(where + ": unknown field " + f);
        }
        for (String f : allowed) if (!n.has(f)) throw new IllegalArgumentException(where + ": missing field " + f);
    }
}
