# Engine assumptions

Spec line 762 lists engine behaviours the adapters rely on and requires them to be confirmed for the pinned versions before delivery
step 6. This file records, per assumption, the pinned artifact, the classes and methods read (line ranges are those of the file inside
the artifact's `-sources` jar), the finding and the test that keeps it true. The plan's rule applies: an unresolved claim is not
confirmed, and a false assumption stops the dependent pull request until a spec correction lands; no gate criterion changes.

The Flink rows are confirmed by executable MiniCluster spikes in `engine-flink/src/test/java/yggdrasil/forseti/engine/flink/spike/`,
which stay as regression tests and run in the per-PR test jobs. The Kafka Streams rows are argued from the 4.2.2 sources; their
executable spikes come with the Kafka Streams adapter (step 7, PR 1).

Pins: `org.apache.flink:flink-streaming-java:2.2.1` (with `flink-runtime`, `flink-core`, `flink-connector-base` 2.2.1),
`org.apache.flink:flink-connector-kafka:5.0.0-2.2`, `org.apache.kafka:kafka-streams:4.2.2`, `org.apache.kafka:kafka-clients:4.2.2`.

## Summary

| Id | Assumption (spec line 762) | Pinned artifact | Classes and methods read | Finding | Test |
|---|---|---|---|---|---|
| F1 | Kafka source split ownership: with source parallelism equal to the number of subscribed partitions, every subscribed partition of both governed placements has its own reader | flink-connector-kafka 5.0.0-2.2 | `KafkaSource.createEnumerator` 202-208; `KafkaSourceEnumerator` constructor 185-222 (default owner selector 201-205), `handlePartitionSplitChanges` 421-438, `addPartitionSplitChangeToPendingAssignments` 441-467, `assignPendingPartitionSplits` 469-511, `getSplitOwner` 599-606 | Confirmed. Owner = `(((topic.hashCode() * 31) & 0x7FFFFFFF) % n + partition) % n`. Every subscribed topic of both placements gets one split per reader (table below); the step-3 placement {1,2,4,5} at n = 4 does not | `KafkaSplitOwnerSpikeTest#everySubscribedPartitionOfAGovernedPlacementHasItsOwnReader`, `#theStep3PlacementCannotGiveEveryReaderOneSplit` |
| F1' | Split-less Kafka readers stay open | flink-connector-kafka 5.0.0-2.2, flink-connector-base 2.2.1 | `KafkaSourceEnumerator.assignPendingPartitionSplits` 503-510; `KafkaSourceReader` 52-54 (extends `SingleThreadMultiplexSourceReaderBase`); `SourceReaderBase.finishedOrAvailableLater` 433-448 | Confirmed by source reading. For an unbounded source the enumerator never sends "no more splits" (only when `BOUNDED`), so a reader without a split returns `NOTHING_AVAILABLE` forever and never ends (no end-of-input `MAX_WATERMARK`) | F2 tests (a source with the same shape) |
| F2 | Split-less reader watermark behaviour | Flink 2.2.1 | `SourceOperator.open` 391-437, `handleOperatorEvent` 654-676; `ProgressiveTimestampsAndWatermarks.createMainOutput` 112-150, `startPeriodicWatermarkEmits` 154-168, `SplitLocalOutputs.createOutputForSplit` 266-303; `WatermarkOutputMultiplexer.getImmediateOutput` 121-126, `updateCombinedWatermark` 157-165, `ImmediateOutput.emitWatermark` 180-189; `CombinedWatermarkStatus.updateCombinedWatermark` 65-107 | Confirmed. With interval 0 and no idleness a reader without a split emits neither a watermark nor an idle status, so at parallelism n+1 over n splits the downstream operator watermark stays `Long.MIN_VALUE`; at n over n it is the minimum over the splits. Line 717 (one split per source subtask) is therefore required | `SplitlessReaderWatermarkSpikeTest#aSplitLessReaderHoldsTheDownstreamWatermarkAtMinValue`, `#nOverNSplitsAdvancesToTheMinimumOverTheSplits` |
| F3 | The `-1` of `BoundedOutOfOrdernessWatermarks` and the delegating generator's per-record emission | Flink 2.2.1 | `BoundedOutOfOrdernessWatermarks` constructor 50-58, `onEvent` 62-65, `onPeriodicEmit` 67-70; `SourceOutputWithWatermarks.collect` 105-117; `ProgressiveTimestampsAndWatermarks.startPeriodicWatermarkEmits` 154-168; `WatermarkOutputMultiplexer.ImmediateOutput.emitWatermark` 180-189 | Confirmed. The built-in bound is `maxTimestamp - outOfOrdernessMs - 1` and is emitted only in `onPeriodicEmit`; interval 0 never schedules the periodic emitter, so the built-in generator alone never advances; the delegating generator makes the watermark after each record `max(ts so far) - outOfOrdernessMs - 1` | `PerRecordWatermarkSpikeTest#theBuiltInBoundIsMaxMinusOutOfOrdernessMinusOneAndOnlyPeriodic`, `#theDelegatingGeneratorEmitsAfterEveryRecordWithIntervalZero`, `#theBuiltInGeneratorAloneNeverAdvancesWithIntervalZero` |
| F4 | Watermarks are not restored after a restore | Flink 2.2.1 | `SourceOperator.snapshotState` 610-614, `initializeState` 634-639; `InternalTimeServiceManagerImpl.create` 106-138; `InternalTimersSnapshot` 33-40; `InternalTimerServiceImpl` field `currentWatermark` 72, `initializeWatermark` 228-230, `restoreTimersForKeyGroup` 380 ff.; `AbstractStreamOperator.processWatermark` 674-693 | Confirmed. The source snapshot holds splits only and the timer snapshot holds serializers and timers only; `initializeWatermark` has no caller in the 2.2.1 runtime sources. After a restore the timers are back, the operator watermark is `Long.MIN_VALUE` at the first record read after the restore and advances only from records read after it | `WatermarkRestoreSpikeTest#theOperatorWatermarkStartsAtMinValueAfterARestore` |
| F5 | `initializeState` runs before the task reports RUNNING | Flink 2.2.1 | `Task.restoreAndInvoke` 930-962; `StreamTask.restore` 778-780, `restoreInternal` 782-847, `restoreStateAndGates` 849-902; `RegularOperatorChain.initializeStateAndOpenOperators` 102-109 | Confirmed. The task switches DEPLOYING to INITIALIZING, runs `restore()` (which calls every operator's `initializeState` and `open`) and only then switches INITIALIZING to RUNNING; a blocked `initializeState` keeps the task INITIALIZING | `InitializeStateBeforeRunningSpikeTest#aBlockedInitializeStateNeverShowsRunning` |
| K1 | Kafka Streams restores partition time from the committed offset metadata | kafka-streams 4.2.2 | `StreamTask.prepareCommit` 457 ff., `findOffsetAndMetadata` 493-509, `committableOffsetsAndMetadata` 511-537, `completeRestoration` 302-337, `resetOffsetsIfNeededAndInitializeMetadata` 1077-1109, `initializeTaskTimeAndProcessorMetadata` 1111-1137; `TopicPartitionMetadata.encode` 53-61, `decode` 63-93; `PartitionGroup.setPartitionTime` 232-241; `RecordQueue.setPartitionTime` 96-98; `TaskExecutor.commitOffsetsOrTransaction` 175-186; `StreamsProducer.commitTransaction` 242-266 | Holds by source reading (argument K1). Executable spike in step 7 | step 7, PR 1 |
| K2 | With `acceptable.recovery.lag` = Long.MAX_VALUE a rebalance assigns a moved stateful task as active without a warm-up replica or a probing rebalance; no eager protocol | kafka-streams 4.2.2, kafka-clients 4.2.2 | `StreamsPartitionAssignor.supportedProtocols` 276-278, assignment 770-857, `createTaskAssignor` 859-872, `populateClientStatesMap` 885-928, `maybeScheduleFollowupRebalance` 1558-1581; `AssignorConfiguration` 81-83; `HighAvailabilityTaskAssignor.assign` 53-115, `assignActiveStatefulTasks` 117-142, `tasksToCaughtUpClients` 256-274, `unbounded` 288-290; `TaskMovement.assignActiveTaskMovements` 77-124, `taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist` 56-68; `FallbackPriorTaskAssignor.assign` 41-50; `ConsumerCoordinator` (clients) 256-259 | No eager protocol: holds. Active without warm-up or probing: holds when the assignor's changelog end-offset lookup succeeds; when it fails the assignor falls back to `FallbackPriorTaskAssignor`, which always requests a follow-up rebalance at `probing.rebalance.interval.ms` (argument K2). The unconditional claim is therefore not confirmed | step 7, PR 1 |
| K3 | RUNNING between cooperative rounds; moves between stream threads; every active-task change through REBALANCING | kafka-streams 4.2.2, kafka-clients 4.2.2 | `KafkaStreams.State` 253-263, `setState` 341-381, `StreamStateListener.maybeSetRunning` 625-644, `onChange` 648-675; `StreamThread.State` 181-186, run loop 950-985, `handleTaskMigrated` 1149-1156, `initializeAndRestorePhase` 1433-1462, `checkStateUpdater` 1464-1471; `StreamsRebalanceListener.onPartitionsAssigned` 53-81, `onPartitionsRevoked` 84-103, `onPartitionsLost` 106-122; `StreamsPartitionAssignor` 1118-1184, `populateActiveTaskAndPartitionsLists` 1192-1240, `assignTasksToThreads` 1293 ff., `onAssignment` 1460-1556; `TaskManager.handleAssignment` 344, `handleLostAll` 1273; `ConsumerCoordinator` (clients) `onJoinComplete` 377-470, `onJoinPrepare` 752 ff. (lost 812-823), `onLeavePrepare` 883-900 | RUNNING between rounds: yes, it can. Moves between threads: yes, they can. Every active-task change through REBALANCING: every change is followed by a REBALANCING transition before the instance reports RUNNING with its new assignment, but on the lost-partitions path the tasks close while the instance still reports RUNNING (argument K3). The strict reading is not confirmed | step 7, PR 1 |

## F1: owners on the governed placements

The jobs subscribe statically (`KafkaSourceBuilder.setPartitions`), partition discovery is off, and `KafkaSource.createEnumerator`
builds the enumerator without an owner selector, so the default selector calls `getSplitOwner`. `handlePartitionSplitChanges` passes
the discovered splits to `addPartitionSplitChangeToPendingAssignments`, which computes the owner with `context.currentParallelism()`
as `numReaders`; `addReader` hands each registered reader its pending splits. `KafkaSource` does not implement
`SupportsSplitReassignmentOnRecovery`, so after a restore a reader keeps the splits of its own state (`SourceOperator.open` 418-430).
The spike drives exactly this path (public constructor, the pending-assignment step, `addReader` per reader) with a recording context;
no broker is needed because discovery is the only step that talks to Kafka. Reader per partition at source parallelism n (topic name
as used by the jobs):

| Placement | Topic | n | partition -> reader |
|---|---|---|---|
| dedicated (site-071) | ot.messages | 5 | 1->3, 2->4, 3->0, 4->1, 5->2 |
| dedicated (site-071) | ot.sessions | 5 | 1->0, 2->1, 3->2, 4->3, 5->4 |
| shared (site-019) | ot.messages | 4 | 0->3, 2->1, 3->2, 5->0 |
| shared (site-019) | ot.sessions | 4 | 0->2, 2->0, 3->1, 5->3 |
| both | mfg.occupancy | 3 | 0->2, 1->0, 2->1 |
| both | quality.incidents | 6 | p->p |

The raw partitions of the declared devices are dedicated 5, 1, 3, 4, 2 and shared 2, 3, 5, 0, 0 (two stations on partition 0), equal
to the subscribed raw partitions of each placement. Because the owner is a rotation of `partition mod n`, the result does not depend on
the topic name whenever the subscribed partitions are pairwise distinct mod n, which holds for every row above; the step-3 placement
{1, 2, 4, 5} at n = 4 has 1 and 5 equal mod 4, so one reader gets two splits and another none for any topic name.

## Engine notes for comparison item 7: keyed subtasks of the stations

The station operators of both jobs run at parallelism 6 with max parallelism 128 (spec 588, plan D6), and a station's subtask is the
one whose key-group range holds `KeyGroupRangeAssignment.assignToKeyGroup(station, 128)`. The key groups and subtasks below are the
ones the stage-1 operators recorded in their init records (step 6 PR 5 probe and runs; the gate asserts the same map on every run,
`FL-KEYGROUP-MAP`, spec 715):

| Placement | Station | Key group | Subtask |
|---|---|---|---|
| dedicated (site-071) | station1 | 115 | 5 |
| dedicated (site-071) | station2 | 122 | 5 |
| dedicated (site-071) | station3 | 118 | 5 |
| shared (site-019) | station1 | 26 | 1 |
| shared (site-019) | station2 | 37 | 1 |
| shared (site-019) | station3 | 20 | 0 |

So on the dedicated placement all three stations are one keyed subtask, and on the shared placement station1 and station2 share
subtask 1 while station3 is on subtask 0; four (dedicated: five) of the six keyed subtasks own no station. Every station output
partition still has exactly one writer, which is what spec 715 requires; the uneven spread is a property of three keys over 128 key
groups, recorded under item 7 as an adapter cost (the keyed parallelism buys no spread for three stations), not a workaround.

## F2 to F5: how the spikes are built

The Flink spikes run on an in-process `MiniCluster` (one TaskManager) with the gate's watermark settings: automatic watermark interval 0
(`pipeline.auto-watermark-interval`), no `withIdleness`, and the thin delegating generator of spec line 77, which wraps
`BoundedOutOfOrdernessWatermarks` and calls its `onPeriodicEmit` from `onEvent`. Their source (`SpikeSource`) is an unbounded FLIP-27
source shaped like the Kafka source where it matters: split `i` goes only to reader `i`, the enumerator never signals "no more splits",
and records go out through the split-local output (`ReaderOutput.createOutputForSplit`), the path `SourceReaderBase` takes (471), so
each split has its own generator under the reader's `WatermarkOutputMultiplexer`. A keyed probe operator records the operator
watermark of its event-time timer service at every record and every watermark it processes; records and watermarks of one channel
arrive in order, so the watermark seen at record k+1 is the one emitted after record k.

- F2 runs two splits at source parallelism 2 and 3. The third reader owns no split, never emits, and holds the probe at
  `Long.MIN_VALUE` after every split has advanced; at parallelism 2 the probe ends at `min(3000, 2000) - 100 - 1`.
- F3 checks the generator directly (no emission in `onEvent`, `max - 100 - 1` in `onPeriodicEmit`, `Long.MIN_VALUE` before any record)
  and in a job over out-of-order timestamps: with the delegating generator the probe sees `max(ts before) - 101` at every record and one
  watermark per record that raised the maximum; with the built-in generator alone it never sees one.
- F4 checkpoints every 50 ms in `EXACTLY_ONCE` mode, stops the source at position 4 until a checkpoint taken there completes, then fails
  once (fixed-delay restart, one attempt). Attempt 1 reports `isRestored` with the restored checkpoint id, resumes at position 4, holds
  the event-time timer registered in attempt 0, sees `Long.MIN_VALUE` at its first record and then only post-restore values.
- F5 blocks an operator's `initializeState` (operator chaining off) and polls the execution graph for one second: the blocked task is
  INITIALIZING at every poll while the source task reaches RUNNING; once released, the blocked task reports RUNNING. Stage 1 reads
  `forseti.recovery-control` in `initializeState` (plan D5), so no stage-1 task is RUNNING before its epoch is fixed.

## K1: partition-time restore from committed offset metadata

1. Commit carries the partition time. `StreamTask.prepareCommit` (457 ff.) returns `committableOffsetsAndMetadata` (511-537) for a
   RUNNING or SUSPENDED task; each entry is built by `findOffsetAndMetadata` (493-509) as
   `OffsetAndMetadata(offset, leaderEpoch, new TopicPartitionMetadata(partitionGroup.partitionTimestamp(partition), processorMetadata).encode())`.
   `encode` (53-61) writes magic byte 2, the partition time as 8 bytes and the processor metadata, Base64.
2. Under `exactly_once_v2` those offsets are committed in the transaction: `TaskExecutor.commitOffsetsOrTransaction` (175-186) passes
   all tasks' offsets to `StreamsProducer.commitTransaction` (242-266), which calls `producer.sendOffsetsToTransaction(offsets, ...)`
   (249) before `commitTransaction` (266); the metadata string travels with each offset.
3. A task newly created on any instance (generation start, restart, move) goes CREATED, RESTORING and then `completeRestoration`
   (302-337), which in RESTORING calls `resetOffsetsIfNeededAndInitializeMetadata` (1077-1109) before `initializeTopology` and
   `transitionTo(RUNNING)`. That method reads `mainConsumer.committed(inputPartitions())` and passes every non-null entry to
   `initializeTaskTimeAndProcessorMetadata` (1111-1137), which decodes the metadata (`TopicPartitionMetadata.decode` 63-93: versions 1
   and 2 carry the partition time) and calls `PartitionGroup.setPartitionTime` (232-241). That sets the queue's partition time
   (`RecordQueue.setPartitionTime` 96-98) and raises the task's stream time to it when larger.
4. So before the task processes a record or a `STREAM_TIME` punctuation, its partition times equal the values committed with the last
   committed offsets, and its stream time is their maximum. A partition with no committed offset, or with empty or undecodable metadata
   (`decode` returns `RecordQueue.UNKNOWN` = -1), starts at -1; the gate's recordings commit on every subscribed partition before any
   restart, which the step-7 spike checks.

## K2: assignment with `acceptable.recovery.lag` = Long.MAX_VALUE

1. No eager protocol. `StreamsPartitionAssignor.supportedProtocols` (276-278) returns only `COOPERATIVE`; `StreamsConfig` always sets
   `partition.assignment.strategy` to `StreamsPartitionAssignor` (1797), and the consumer coordinator uses the protocols common to its
   assignors (clients `ConsumerCoordinator` 256-259). The string `EAGER` does not occur in the kafka-streams 4.2.2 main sources, so no
   `upgrade.from` value selects it. (`group.protocol=classic` is pinned by spec line 713; the server-side `streams` protocol is not
   used.)
2. Which assignor runs. With `task.assignor.class` unset (`AssignorConfiguration.customTaskAssignor` 148-151) the legacy path runs, and
   the internal assignor defaults to `HighAvailabilityTaskAssignor` (`AssignorConfiguration` 81-83). `createTaskAssignor` (859-872)
   returns it only when `populateClientStatesMap` (885-928) succeeded in fetching the changelog end offsets; otherwise it returns
   `FallbackPriorTaskAssignor`.
3. Normal path, no warm-up and no probing. `tasksToCaughtUpClients` (256-274) puts every client in the caught-up set of every stateful
   task when `unbounded(acceptableRecoveryLag)` (288-290), i.e. Long.MAX_VALUE. `assignActiveStatefulTasks` (117-142) assigns each
   stateful task as active round-robin over the sorted clients and balances; `assignActiveTaskMovements` (77-124) schedules a movement
   only for a task that `taskIsNotCaughtUpOnClientAndOtherMoreCaughtUpClientsExist` (56-68), which returns false at once for a caught-up
   client, so no movement and no warm-up is created. With `num.standby.replicas` = 0 there are no standby tasks to move. `assign`
   returns `probingRebalanceNeeded = movements > 0 = false` (106), so no follow-up deadline is encoded (770-857, 1118-1184) and
   `maybeScheduleFollowupRebalance` (1558-1581) sets no probing rebalance. A task of a killed instance, whose partitions no live member
   owns, is assigned active in the same rebalance.
4. Fallback path. If the end-offset lookup throws `StreamsException` or `TimeoutException` (911-916), `FallbackPriorTaskAssignor.assign`
   (41-50) assigns with `LegacyStickyTaskAssignor` and always returns true; the leader then encodes a follow-up rebalance at
   `probing.rebalance.interval.ms` (833-841). The moved task is still active without a warm-up, but a probing-interval rebalance
   follows, so the claim holds only when the lookup succeeds. Not confirmed in its unconditional form; this needs a spec decision
   before the Kafka Streams adapter.
5. Note for step 7: on the normal path the active assignment is a function of the sorted client ids and tasks, not of the previous
   owners, so a rebalance after a restart can also move tasks between live instances; such a move takes two cooperative rounds
   (`populateActiveTaskAndPartitionsLists` 1192-1240 withholds a partition until its owner revoked it and requests an immediate
   follow-up, 1160-1166). The stable-assignment rule of spec line 714 covers it.

## K3: state transitions around rebalances

1. RUNNING between the rounds of a cooperative rebalance: yes, it can. When a task changes owner, round 1 withholds it from the new owner
   and encodes an immediate follow-up (1160-1166), and the old owner revokes it (`onJoinComplete` 438-448, then `requestRejoin`). Each
   instance ends round 1 in `onPartitionsAssigned`, the thread goes to PARTITIONS_ASSIGNED and the instance to REBALANCING
   (`StreamStateListener.onChange` 656-657). The thread runs `runOnce` before it acts on the follow-up: `initializeAndRestorePhase`
   (1433-1462) or `checkStateUpdater` (1464-1471) set the thread RUNNING once its current tasks are restored, and `maybeSetRunning`
   (625-644) then sets the instance RUNNING; only afterwards does the run loop call `enforceRebalance` for the scheduled follow-up
   (961-965), and the rejoin sends the thread back to PARTITIONS_ASSIGNED. Whether RUNNING appears between the rounds depends on
   restoration time, so the stable-assignment rule, not a single transition, ends a rebalance in the gate.
2. Moves between stream threads of one instance: yes, they can. The assignor hands tasks to consumers, one per stream thread:
   `assignTasksToThreads` (1293 ff.) gives each thread its previous tasks up to the per-thread minimum and interleaves the rest. With
   `num.stream.threads` = 1 every task of an instance belongs to its only thread.
3. Every change to an instance's active tasks takes it through REBALANCING: holds only in the "followed by" sense.
   - Revocation: `onJoinComplete` calls `onPartitionsRevoked` (443) before the assignor; the listener sets PARTITIONS_REVOKED first and
     only then calls `taskManager.handleRevocation` (95-98). REBALANCING precedes the change.
   - Addition: `onJoinComplete` calls `assignor.onAssignment` (457), which runs `taskManager.handleAssignment` (1555), before
     `onPartitionsAssigned` (466), which the consumer invokes after every join even with no added partition. The change precedes
     REBALANCING by a few statements inside the same `poll()`, before any record of the new task is processed.
   - Lost partitions: `onPartitionsLost` (`onJoinPrepare` with a reset generation 812-823, `onLeavePrepare` 883-900) and
     `StreamThread.handleTaskMigrated` (1149-1156) call `taskManager.handleLostAll` (1273), which closes every active task with no thread
     transition. The instance keeps reporting RUNNING until the rejoin completes and `onPartitionsAssigned` sets REBALANCING.
   - `KafkaStreams.setState` (341-381) refuses REBALANCING to REBALANCING, so a change inside an ongoing rebalance adds no transition.
   So every change is followed by a REBALANCING transition before the instance reports RUNNING with its new assignment, but on the lost
   path the instance reports RUNNING after its tasks were closed. The strict reading is not confirmed; this needs a spec decision before
   the Kafka Streams adapter.
