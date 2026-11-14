package yggdrasil.forseti.loop;

import java.util.List;
import yggdrasil.forseti.cli.live.DrivePlan;

/** The driver's view of the Bifrost sim: one Read request and one Write request, nothing else. The Milo client implements it
 *  ({@link MiloSimPort}); the tests fake it, scans included, so the driver's timing rules run without a sim.
 *
 *  <p>Why only these two calls: the sim's contract with the driver is per request (step-4 plan Decisions 3, 5, 6): the accepted items
 *  of one Write request reach the members in one scan, every member of one scan carries one source timestamp, and a Read is not
 *  serialized with the scan, so a Read may straddle two scans and show two source timestamps. */
public interface SimPort extends AutoCloseable {
    /** One node as read. {@code exists} is false when the server does not know the node; {@code sourceTs} is the source timestamp in
     *  epoch milliseconds, or null when the server gave none; {@code statusCode} is the value's OPC UA status code. */
    record Value(boolean exists, Object value, long statusCode, Long sourceTs) {}

    /** One Read request of the Value attribute of {@code identifiers} (string node ids in namespace {@code namespaceUri}), with maxAge 0. */
    List<Value> read(String namespaceUri, List<String> identifiers);

    /** One Write request of every item; the per-item status codes in item order (0 = Good). */
    List<Long> write(String namespaceUri, List<DrivePlan.Write> items);

    @Override void close();
}
