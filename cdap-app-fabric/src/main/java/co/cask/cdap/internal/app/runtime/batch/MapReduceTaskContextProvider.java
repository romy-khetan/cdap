/*
 * Copyright © 2014-2016 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.cdap.internal.app.runtime.batch;

import co.cask.cdap.api.mapreduce.MapReduceSpecification;
import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.metrics.MetricsContext;
import co.cask.cdap.app.metrics.MapReduceMetrics;
import co.cask.cdap.app.program.DefaultProgram;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.metadata.writer.ProgramContextAware;
import co.cask.cdap.internal.app.runtime.workflow.NameMappedDatasetFramework;
import co.cask.cdap.internal.app.runtime.workflow.WorkflowProgramInfo;
import co.cask.cdap.proto.Id;
import co.cask.tephra.TransactionSystemClient;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.inject.Injector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.MRConfig;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.apache.twill.filesystem.LocalLocationFactory;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import javax.annotation.Nullable;

/**
 * Provides access to MapReduceTaskContext for mapreduce job tasks.
 */
public class MapReduceTaskContextProvider extends AbstractIdleService {

  private static final Logger LOG = LoggerFactory.getLogger(MapReduceTaskContextProvider.class);
  private final Injector injector;
  // Maintain a cache of taskId to MapReduceTaskContext
  // Each task should have it's own instance of MapReduceTaskContext so that different dataset instance will
  // be created for different task, which is needed in local mode since job runs with multiple threads
  private final LoadingCache<ContextCacheKey, BasicMapReduceTaskContext> taskContexts;

  /**
   * Helper method to tell if the MR is running in local mode or not. This method doesn't really belongs to this
   * class, but currently there is no better place for it.
   */
  static boolean isLocal(Configuration hConf) {
    String mrFramework = hConf.get(MRConfig.FRAMEWORK_NAME, MRConfig.LOCAL_FRAMEWORK_NAME);
    return MRConfig.LOCAL_FRAMEWORK_NAME.equals(mrFramework);
  }

  /**
   * Creates an instance with the given {@link Injector} that will be used for getting service instances.
   */
  protected MapReduceTaskContextProvider(Injector injector) {
    this.injector = injector;
    this.taskContexts = CacheBuilder.newBuilder().build(createCacheLoader(injector));
  }

  protected Injector getInjector() {
    return injector;
  }

  @Override
  protected void startUp() throws Exception {
    // no-op
  }

  @Override
  protected void shutDown() throws Exception {
    // Close all the contexts to release resources
    for (BasicMapReduceTaskContext context : taskContexts.asMap().values()) {
      try {
        context.close();
      } catch (Exception e) {
        LOG.warn("Exception when closing context {}", context, e);
      }
    }
  }

  /**
   * Returns the {@link BasicMapReduceTaskContext} for the given task.
   */
  public final <K, V> BasicMapReduceTaskContext<K, V> get(TaskAttemptContext taskAttemptContext) {
    return get(new ContextCacheKey(taskAttemptContext));
  }

  /**
   * Returns the {@link BasicMapReduceTaskContext} for the given configuration. Since TaskAttemptContext is not
   * provided, the returned MapReduceTaskContext will not have Metrics available.
   */
  public final <K, V> BasicMapReduceTaskContext<K, V> get(Configuration configuration) {
    // TODO: still create a metric context, but it just omits task id and task type?
    return get(new ContextCacheKey(null, configuration));
  }

  private <K, V> BasicMapReduceTaskContext<K, V> get(ContextCacheKey key) {
    @SuppressWarnings("unchecked")
    BasicMapReduceTaskContext<K, V> context = (BasicMapReduceTaskContext<K, V>) taskContexts.getUnchecked(key);
    return context;
  }

  /**
   * Creates a {@link Program} instance based on the information from the {@link MapReduceContextConfig}, using
   * the given program ClassLoader.
   */
  private Program createProgram(MapReduceContextConfig contextConfig, ClassLoader programClassLoader) {
    Location programLocation;
    LocationFactory locationFactory = new LocalLocationFactory();

    if (isLocal(contextConfig.getHConf())) {
      // Just create a local location factory. It's for temp usage only as the program location is always absolute.
      programLocation = locationFactory.create(contextConfig.getProgramJarURI());
    } else {
      // In distributed mode, the program jar is localized to the container
      programLocation = locationFactory.create(new File(contextConfig.getProgramJarName()).getAbsoluteFile().toURI());
    }

    return new DefaultProgram(new ProgramDescriptor(contextConfig.getProgramId(),
                                                    contextConfig.getApplicationSpecification()),
                              programLocation, programClassLoader);
  }

  /**
   * Creates a {@link CacheLoader} for the task context cache.
   */
  private CacheLoader<ContextCacheKey, BasicMapReduceTaskContext> createCacheLoader(final Injector injector) {
    final DiscoveryServiceClient discoveryServiceClient = injector.getInstance(DiscoveryServiceClient.class);
    final DatasetFramework datasetFramework = injector.getInstance(DatasetFramework.class);
    // Multiple instances of BasicMapReduceTaskContext can shares the same program.
    final AtomicReference<Program> programRef = new AtomicReference<>();

    return new CacheLoader<ContextCacheKey, BasicMapReduceTaskContext>() {
      @Override
      public BasicMapReduceTaskContext load(ContextCacheKey key) throws Exception {
        MapReduceContextConfig contextConfig = new MapReduceContextConfig(key.getConfiguration());
        MapReduceClassLoader classLoader = MapReduceClassLoader.getFromConfiguration(key.getConfiguration());

        Program program = programRef.get();
        if (program == null) {
          // Creation of program is relatively cheap, so just create and do compare and set.
          programRef.compareAndSet(null, createProgram(contextConfig, classLoader.getProgramClassLoader()));
          program = programRef.get();
        }

        WorkflowProgramInfo workflowInfo = contextConfig.getWorkflowProgramInfo();
        DatasetFramework programDatasetFramework = workflowInfo == null ?
          datasetFramework :
          NameMappedDatasetFramework.createFromWorkflowProgramInfo(datasetFramework, workflowInfo,
                                                                   program.getApplicationSpecification());

        // Setup dataset framework context, if required
        if (programDatasetFramework instanceof ProgramContextAware) {
          Id.Run id = new Id.Run(program.getId(), contextConfig.getRunId().getId());
          ((ProgramContextAware) programDatasetFramework).initContext(id);
        }

        MapReduceSpecification spec = program.getApplicationSpecification().getMapReduce().get(program.getName());

        MetricsContext metricsContext = null;

        TaskAttemptID taskAttemptID = key.getTaskAttemptID();
        // taskAttemptId can be null, if used from a Partitioner, (TODO: add other types here)
        if (taskAttemptID != null) {
          String taskId = taskAttemptID.getTaskID().toString();
          // this can be false (non mapper or reducer task type)... TODO: figure out when that is....
          if (MapReduceMetrics.TaskType.hasType(taskAttemptID.getTaskType())) {
            MapReduceMetrics.TaskType taskType = MapReduceMetrics.TaskType.from(taskAttemptID.getTaskType());
            // if this is not for a mapper or a reducer, we don't need the metrics collection service
            MetricsCollectionService metricsCollectionService = injector.getInstance(MetricsCollectionService.class);

            metricsContext =
              BasicMapReduceTaskContext.createMetricsContext(program, contextConfig.getRunId().getId(),
                                                             metricsCollectionService, taskId, taskType, workflowInfo);
          }
        }

        TransactionSystemClient txClient = injector.getInstance(TransactionSystemClient.class);


        return new BasicMapReduceTaskContext(
          program, contextConfig.getRunId(), contextConfig.getArguments(), spec,
          workflowInfo, discoveryServiceClient, txClient, contextConfig.getTx(),
          programDatasetFramework, classLoader.getPluginInstantiator(),
          contextConfig.getLocalizedResources(), metricsContext
        );
      }
    };
  }

  /**
   * Private class to represent the caching key for the {@link BasicMapReduceTaskContext} instances.
   */
  private static final class ContextCacheKey {

    private final TaskAttemptID taskAttemptID;
    private final Configuration configuration;

    private ContextCacheKey(TaskAttemptContext context) {
      this(context.getTaskAttemptID(), context.getConfiguration());
    }

    public ContextCacheKey(@Nullable TaskAttemptID taskAttemptID, Configuration configuration) {
      this.taskAttemptID = taskAttemptID;
      this.configuration = configuration;
    }

    @Nullable
    public TaskAttemptID getTaskAttemptID() {
      return taskAttemptID;
    }

    public Configuration getConfiguration() {
      return configuration;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      // Only compares with the task ID
      ContextCacheKey that = (ContextCacheKey) o;
      return Objects.equals(taskAttemptID, that.taskAttemptID);
    }

    @Override
    public int hashCode() {
      return Objects.hash(taskAttemptID);
    }
  }
}
