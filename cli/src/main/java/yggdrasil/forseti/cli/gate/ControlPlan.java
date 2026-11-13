package yggdrasil.forseti.cli.gate;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import yggdrasil.forseti.cli.harness.Controls;
import yggdrasil.forseti.core.Json;
import yggdrasil.forseti.core.canon.Canonical;

/** The committed negative controls of the core gate ({@code gate/core-negative-controls.json}, plan Task 22): per control its intended
 *  failures and the shards of {@code gate/core-shards.json} it runs on. Owner decision of 2026-11-12: a control runs only on the shards
 *  that own its intended ids or feed them (an id of the plan's {@code aggregate} list is fed by the shards whose runs it is evaluated
 *  over, {@link #feeds}); its intended ids must be a subset of the failed ones; a crash or a missing run never counts as an intended
 *  failure; a control report never authorizes a promotion; every cut and oracle comparison is kept.
 *
 *  <p>Format: {@code {"controls": [{"name", "intended": [ids], "shards": [names]}]}}. {@link #validate} refuses a file whose controls
 *  are not exactly the ones this gate implements ({@link CoreGate#IMPLEMENTED_CONTROLS}), whose intended ids differ from the
 *  implementation's or are not in the manifest, a control without a shard, a shard that is not in the plan or owns and feeds none of
 *  its control's intended ids, and an intended id that no listed shard owns (a shard id) or feeds (an aggregate id). */
final class ControlPlan {
    record Control(String name, List<String> intended, List<String> shards) {
        /** The intended ids a run of {@code layout} evaluates: a shard run its shard's, an aggregate run the plan's aggregate ones, a full
         *  run all of them. */
        List<String> owned(Shards.Plan plan, String shard, boolean aggregate) {
            if (shard != null) { List<String> mine = plan.shard(shard).assertions(); return intended.stream().filter(mine::contains).toList(); }
            if (aggregate) return intended.stream().filter(plan.aggregate()::contains).toList();
            return intended;
        }
        /** The intended ids that are aggregate ids of {@code plan}: the ones an aggregate control run over this control's shards proves. */
        List<String> aggregateIds(Shards.Plan plan) { return intended.stream().filter(plan.aggregate()::contains).toList(); }
    }

    final List<Control> controls;
    final String sha256;

    private ControlPlan(List<Control> controls, String sha256) { this.controls = controls; this.sha256 = sha256; }

    static ControlPlan load(Path root) {
        byte[] bytes;
        try { bytes = Files.readAllBytes(root.resolve(CoreGate.CONTROLS)); }
        catch (IOException e) { throw new IllegalArgumentException("cannot read " + CoreGate.CONTROLS + ": " + e); }
        JsonNode n;
        try { n = Json.read(bytes); } catch (IllegalArgumentException e) { throw new IllegalArgumentException("cannot read " + CoreGate.CONTROLS + ": " + e.getMessage()); }
        if (!n.path("controls").isArray()) throw new IllegalArgumentException(CoreGate.CONTROLS + ": controls is not an array");
        List<Control> l = new ArrayList<>();
        for (JsonNode c : n.path("controls")) {
            if (!c.isObject()) throw new IllegalArgumentException(CoreGate.CONTROLS + ": a control is not an object: " + c);
            l.add(new Control(Json.text(c, "name"), texts(c, "intended"), texts(c, "shards")));
        }
        return new ControlPlan(List.copyOf(l), Canonical.digestOf(bytes));
    }

    static List<String> texts(JsonNode n, String field) {
        if (!n.path(field).isArray()) throw new IllegalArgumentException(CoreGate.CONTROLS + ": " + field + " of " + n.path("name") + " is not an array");
        List<String> l = new ArrayList<>();
        for (JsonNode x : n.path(field)) {
            if (!x.isTextual()) throw new IllegalArgumentException(CoreGate.CONTROLS + ": " + field + " of " + n.path("name") + " holds " + x);
            l.add(x.textValue());
        }
        return List.copyOf(l);
    }

    List<String> names() { return controls.stream().map(Control::name).toList(); }

    /** The control named {@code name}; a usage error when the file does not list it. */
    Control control(String name) {
        for (Control c : controls) if (c.name().equals(name)) return c;
        throw new IllegalArgumentException("--negative-control " + name + ": not in " + CoreGate.CONTROLS + " " + names());
    }

    /** Whether shard {@code shard} of {@code plan} feeds aggregate id {@code id}: the scenario runs the id is evaluated over include one
     *  the shard makes. The {@code J-*} integrity aggregates and {@code C-1} are evaluated over every run of the gate, so every shard
     *  feeds them; {@code J-AFTER-HORIZON-CUTS} is fed by the shards that make a horizon scenario; {@code C-2-REFERENCE} by the shards that
     *  make {@code held-back-10min} or {@code horizon-planned-late}. */
    static boolean feeds(Shards.Plan plan, String shard, String id) {
        if (!plan.aggregate().contains(id)) return false;
        Shards.Shard s = plan.shard(shard);
        if (Injections.J.contains(id) || Injections.ITEM1.equals(id)) return !s.scenarios().isEmpty();
        if (id.equals("J-AFTER-HORIZON-CUTS")) return s.scenarios().stream().anyMatch(Scenarios.HORIZON::contains);
        if (id.equals("C-2-REFERENCE")) return s.scenarios().contains("held-back-10min") || s.scenarios().contains("horizon-planned-late");
        return false;
    }

    /** What is wrong with this file against the shard plan, the manifest ids and the controls this gate implements. */
    List<String> validate(Shards.Plan plan, List<String> manifestIds) {
        List<String> p = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (Control c : controls) {
            String n = c.name();
            if (!seen.add(n)) p.add("control " + n + " is listed twice");
            if (!Controls.NAMES.contains(n) || !CoreGate.IMPLEMENTED_CONTROLS.containsKey(n)) { p.add("control " + n + " is not implemented by this gate"); continue; }
            List<String> want = CoreGate.intended(n, manifestIds);
            if (!new TreeSet<>(want).equals(new TreeSet<>(c.intended())) || new HashSet<>(c.intended()).size() != c.intended().size())
                p.add("control " + n + ": intended " + c.intended() + ", the implementation intends " + want);
            if (c.intended().isEmpty()) p.add("control " + n + " intends no failure");
            for (String id : c.intended()) if (!manifestIds.contains(id)) p.add("control " + n + ": intended " + id + " is not in the manifest");
            if (c.shards().isEmpty()) p.add("control " + n + " runs on no shard");
            if (new HashSet<>(c.shards()).size() != c.shards().size()) p.add("control " + n + " lists a shard twice");
            List<String> known = new ArrayList<>();
            for (String s : c.shards()) {
                if (!plan.names().contains(s)) { p.add("control " + n + ": shard " + s + " is not in " + Shards.PLAN); continue; }
                known.add(s);
                List<String> mine = plan.shard(s).assertions();
                boolean useful = false;
                for (String id : c.intended()) if (mine.contains(id) || feeds(plan, s, id)) useful = true;
                if (!useful) p.add("control " + n + ": shard " + s + " owns and feeds none of its intended ids " + c.intended());
            }
            for (String id : c.intended()) {
                if (plan.aggregate().contains(id)) {
                    if (known.stream().noneMatch(s -> feeds(plan, s, id))) p.add("control " + n + ": no listed shard feeds its aggregate id " + id);
                } else if (known.stream().noneMatch(s -> plan.shard(s).assertions().contains(id))) p.add("control " + n + ": no listed shard owns " + id);
            }
        }
        for (String n : CoreGate.IMPLEMENTED_CONTROLS.keySet()) if (!seen.contains(n)) p.add("control " + n + " is implemented but not listed");
        return p;
    }
}
