package yggdrasil.forseti.engine.flink.job;

import java.util.Arrays;
import org.apache.flink.api.common.JobID;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.configuration.PipelineOptionsInternal;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.resourcemanager.StandaloneResourceManagerFactory;

/**
 * The JobManager JVM of one job in application mode (plan D7): one application cluster per job, its job's {@code main} run inside it,
 * no Flink distribution and no Docker image. It is the standalone application entry point of the Flink distribution
 * ({@code StandaloneApplicationClusterEntryPoint}, which is not published as a Maven artifact) reduced to what the harness needs:
 * {@code <configDir> <jobId> <jobClass> <jobArgs...>}, the job class found on this JVM's own class path, the job id fixed so the
 * harness can address the job over REST from the start. The TaskManager JVM is Flink's own
 * {@code org.apache.flink.runtime.taskexecutor.TaskManagerRunner} on the same configuration directory. When the job reaches a terminal
 * state the cluster shuts down and this JVM exits.
 */
public final class ApplicationEntryPoint extends ApplicationClusterEntryPoint {
  private ApplicationEntryPoint(Configuration configuration, PackagedProgram program) {
    super(configuration, program, StandaloneResourceManagerFactory.getInstance());
  }

  public static void main(String[] args) {
    try {
      if (args.length < 3) throw new IllegalArgumentException("usage: ApplicationEntryPoint <configDir> <jobId> <jobClass> [jobArgs...]");
      Configuration conf = GlobalConfiguration.loadConfiguration(args[0]);
      conf.set(PipelineOptionsInternal.PIPELINE_FIXED_JOB_ID, JobID.fromHexString(args[1]).toHexString());
      PackagedProgram program = DefaultPackagedProgramRetriever.create(null, args[2], Arrays.copyOfRange(args, 3, args.length), conf).getPackagedProgram();
      configureExecution(conf, program);
      ClusterEntrypoint.runClusterEntrypoint(new ApplicationEntryPoint(conf, program));
    } catch (Throwable t) {
      System.err.println("forseti application entry point failed: " + t);
      t.printStackTrace();
      System.exit(1);
    }
  }
}
