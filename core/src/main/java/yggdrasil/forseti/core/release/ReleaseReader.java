package yggdrasil.forseti.core.release;

import com.fasterxml.jackson.databind.JsonNode;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

/** Reads the release a verified head names, at Vor's level (Decision 14): release.json by raw-byte SHA-256 against the head, every
 *  listed payload file by its own raw-byte SHA-256, and the bundle's proposal against the release file list. Bytes are read once,
 *  at verification, and served from memory afterwards, so a file changed after verification is never read. */
public final class ReleaseReader {
    private ReleaseReader() {}

    public record DocKey(String kind, String id) {}
    public record Doc(String kind, String id, String version, String path, String sha256, JsonNode node) {}
    public record Ref(String id, String version) {}

    public record Release(HeadVerifier.Head head, String root, JsonNode doc, Map<String, String> files, Map<DocKey, Doc> documents, Map<String, byte[]> payload) {
        /** The document of {@code kind} named by {@code ref}; a missing document or another version is provenance-mismatch. */
        public Doc document(String kind, Ref ref) {
            Doc d = documents.get(new DocKey(kind, ref.id()));
            if (d == null || !d.version().equals(ref.version()))
                throw new Refusal("provenance-mismatch", "rule", "document-missing", "kind", kind, "id", ref.id(), "version", ref.version());
            return d;
        }
        /** The verified bytes of a payload path (relative to payload/); a path the release does not list is refused. */
        public byte[] payload(String path) {
            byte[] b = payload.get("payload/" + path);
            if (b == null) throw new Refusal("provenance-mismatch", "rule", "unlisted", "path", path);
            return b.clone();
        }
        public List<Doc> documentsOfKind(String kind) {
            return documents.values().stream().filter(d -> d.kind().equals(kind)).sorted(Comparator.comparing(Doc::id)).toList();
        }
    }

    public static Release read(Path mappings, HeadVerifier.Head head) {
        String root = "releases/" + head.releaseId() + "/" + head.version();
        Path dir = mappings.resolve(root);
        byte[] releaseBytes = bytes(dir.resolve("release.json"), "release-digest", root + "/release.json");
        if (!Canonical.digestOf(releaseBytes).equals(head.releaseSha256()))
            throw new Refusal("provenance-mismatch", "rule", "release-digest", "expected", head.releaseSha256(), "actual", Canonical.digestOf(releaseBytes));
        JsonNode doc;
        try { doc = Json.read(releaseBytes); } catch (IllegalArgumentException e) { throw new Refusal("provenance-mismatch", "rule", "release-digest", "reason", "not-json"); }
        if (!head.releaseId().equals(doc.path("id").asText(null)) || !head.version().equals(doc.path("version").asText(null)) || !head.stream().equals(doc.path("streamId").asText(null)))
            throw new Refusal("provenance-mismatch", "rule", "release-identity", "id", doc.path("id").asText(""), "version", doc.path("version").asText(""), "streamId", doc.path("streamId").asText(""));
        if (!doc.path("files").isArray()) throw new Refusal("provenance-mismatch", "rule", "release-path", "reason", "files");

        Map<String, String> files = new LinkedHashMap<>();
        Map<String, byte[]> payload = new LinkedHashMap<>();
        for (JsonNode f : doc.path("files")) {
            String path = f.path("path").asText(null), sha = f.path("sha256").asText(null);
            if (path == null || sha == null || !normalised(path) || !path.startsWith("payload/") || files.containsKey(path))
                throw new Refusal("provenance-mismatch", "rule", "release-path", "path", String.valueOf(path));
            byte[] b = bytes(dir.resolve(path), "release-file", path);
            if (!Canonical.digestOf(b).equals(sha)) throw new Refusal("provenance-mismatch", "rule", "release-file", "path", path, "expected", sha, "actual", Canonical.digestOf(b));
            files.put(path, sha); payload.put(path, b);
        }

        byte[] proposalBytes = payload.get("payload/proposal.json");
        if (proposalBytes == null) throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", "proposal.json");
        Map<DocKey, Doc> documents = new LinkedHashMap<>();
        JsonNode proposal;
        try { proposal = Json.read(proposalBytes); } catch (IllegalArgumentException e) { throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", "proposal.json", "reason", "not-json"); }
        if (!proposal.path("files").isArray()) throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", "proposal.json", "reason", "files");
        for (JsonNode e : proposal.path("files")) {
            String path, kind, id, version, sha;
            try { path = Json.text(e, "path"); kind = Json.text(e, "kind"); id = Json.text(e, "id"); version = Json.text(e, "version"); sha = Json.text(e, "sha256"); }
            catch (IllegalArgumentException x) { throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", "proposal.json", "reason", "entry"); }
            if (!sha.equals(files.get("payload/" + path))) throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", path);
            JsonNode node;
            try { node = Json.read(payload.get("payload/" + path)); } catch (IllegalArgumentException x) { throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", path, "reason", "not-json"); }
            if (documents.put(new DocKey(kind, id), new Doc(kind, id, version, path, sha, node)) != null)
                throw new Refusal("provenance-mismatch", "rule", "proposal-file", "path", path, "reason", "duplicate document");
        }
        return new Release(head, root, doc, Collections.unmodifiableMap(files), Collections.unmodifiableMap(documents), Collections.unmodifiableMap(payload));
    }

    static boolean normalised(String path) {
        if (path.isEmpty() || path.startsWith("/") || path.contains("\\") || path.contains("//")) return false;
        for (String part : path.split("/")) if (part.isEmpty() || part.equals(".") || part.equals("..")) return false;
        return true;
    }
    static byte[] bytes(Path p, String rule, String name) {
        try { return Files.readAllBytes(p); } catch (IOException e) { throw new Refusal("provenance-mismatch", "rule", rule, "path", name, "reason", "missing"); }
    }
}
