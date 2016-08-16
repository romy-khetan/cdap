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

package co.cask.cdap.internal.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.DatasetInstantiationException;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.dataset.DatasetAdmin;
import co.cask.cdap.api.dataset.DatasetDefinition;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.flow.FlowletConnection;
import co.cask.cdap.api.flow.FlowletDefinition;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.service.ServiceSpecification;
import co.cask.cdap.api.worker.WorkerSpecification;
import co.cask.cdap.api.workflow.WorkflowActionNode;
import co.cask.cdap.api.workflow.WorkflowNode;
import co.cask.cdap.api.workflow.WorkflowSpecification;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.app.store.Store;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.data.dataset.SystemDatasetInstantiator;
import co.cask.cdap.data2.datafabric.dataset.DatasetsUtil;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.data2.dataset2.MultiThreadDatasetCache;
import co.cask.cdap.internal.app.ForwardingApplicationSpecification;
import co.cask.cdap.internal.app.ForwardingFlowSpecification;
import co.cask.cdap.proto.BasicThrowable;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.ProgramType;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.tephra.TransactionAware;
import org.apache.tephra.TransactionExecutor;
import org.apache.tephra.TransactionExecutorFactory;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.api.RunId;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Implementation of the Store that ultimately places data into MetaDataTable.
 */
public class DefaultStore implements Store {

  // mds is specific for metadata, we do not want to add workflow stats related information to the mds,
  // as it is not specifically metadata
  public static final String WORKFLOW_STATS_TABLE = "workflow.stats";
  private static final Logger LOG = LoggerFactory.getLogger(DefaultStore.class);
  private static final Id.DatasetInstance APP_META_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, Constants.AppMetaStore.TABLE);
  private static final Id.DatasetInstance WORKFLOW_STATS_INSTANCE_ID =
    Id.DatasetInstance.from(Id.Namespace.SYSTEM, WORKFLOW_STATS_TABLE);
  private static final Gson GSON = new Gson();
  private static final Map<String, String> EMPTY_STRING_MAP = ImmutableMap.of();
  private static final Type STRING_MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();

  private final LocationFactory locationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final CConfiguration configuration;
  private final DatasetFramework dsFramework;

  private final Supplier<AppMetadataStore> apps;
  private final Supplier<WorkflowDataset> workflows;
  private final Supplier<TransactionExecutor> appsTx;
  private final Supplier<TransactionExecutor> workflowsTx;
  private final MultiThreadDatasetCache dsCache;

  @Inject
  public DefaultStore(CConfiguration conf,
                      LocationFactory locationFactory,
                      NamespacedLocationFactory namespacedLocationFactory,
                      final TransactionExecutorFactory txExecutorFactory,
                      DatasetFramework framework,
                      TransactionSystemClient txClient) {
    this.configuration = conf;
    this.locationFactory = locationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.dsFramework = framework;
    this.dsCache = new MultiThreadDatasetCache(
      new SystemDatasetInstantiator(framework, null, null), txClient,
      NamespaceId.SYSTEM, ImmutableMap.<String, String>of(), null, null);
    this.apps =
      new Supplier<AppMetadataStore>() {
        @Override
        public AppMetadataStore get() {
          Table table = getCachedOrCreateTable(APP_META_INSTANCE_ID.getId());
          return new AppMetadataStore(table, configuration);
        }
      };
    this.appsTx = new Supplier<TransactionExecutor>() {
      @Override
      public TransactionExecutor get() {
        return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) apps.get()));
      }
    };
    this.workflows =
      new Supplier<WorkflowDataset>() {
        @Override
        public WorkflowDataset get() {
          Table table = getCachedOrCreateTable(WORKFLOW_STATS_INSTANCE_ID.getId());
          return new WorkflowDataset(table);
        }
      };
    this.workflowsTx = new Supplier<TransactionExecutor>() {
      @Override
      public TransactionExecutor get() {
        return txExecutorFactory.createExecutor(ImmutableList.of((TransactionAware) workflows.get()));
      }
    };
  }

  /**
   * Adds datasets and types to the given {@link DatasetFramework} used by app mds.
   *
   * @param framework framework to add types and datasets to
   */
  public static void setupDatasets(DatasetFramework framework) throws IOException, DatasetManagementException {
    framework.addInstance(Table.class.getName(), APP_META_INSTANCE_ID, DatasetProperties.EMPTY);
    framework.addInstance(Table.class.getName(), WORKFLOW_STATS_INSTANCE_ID, DatasetProperties.EMPTY);
  }

  private Table getCachedOrCreateTable(String name) {
    try {
      return dsCache.getDataset(name);
    } catch (DatasetInstantiationException e) {
      try {
        DatasetsUtil.getOrCreateDataset(
          dsFramework, Id.DatasetInstance.from(Id.Namespace.SYSTEM, name), "table",
          DatasetProperties.EMPTY, DatasetDefinition.NO_ARGUMENTS, null);
        return dsCache.getDataset(name);
      } catch (DatasetManagementException | IOException e1) {
        throw Throwables.propagate(e);
      }
    }
  }

  @Override
  public ProgramDescriptor loadProgram(final Id.Program id) throws IOException, ApplicationNotFoundException,
                                                                   ProgramNotFoundException {
    ApplicationMeta appMeta = appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, ApplicationMeta>() {
        @Override
        public ApplicationMeta apply(AppMetadataStore mds) throws Exception {
          return mds.getApplication(id.getNamespaceId(), id.getApplicationId());
        }
      }, apps.get());

    if (appMeta == null) {
      throw new ApplicationNotFoundException(Id.Application.from(id.getNamespaceId(), id.getApplicationId()));
    }

    if (!programExists(id, appMeta.getSpec())) {
      throw new ProgramNotFoundException(id);
    }

    return new ProgramDescriptor(id.toEntityId(), appMeta.getSpec());
  }

  @Override
  public void compareAndSetStatus(final Id.Program id, final String pid, final ProgramRunStatus expectedStatus,
                                  final ProgramRunStatus newStatus) {
    Preconditions.checkArgument(expectedStatus != null, "Expected of program run should be defined");
    Preconditions.checkArgument(newStatus != null, "New state of program run should be defined");
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        RunRecordMeta target = mds.getRun(id, pid);
        if (target.getStatus() == expectedStatus) {
          long now = System.currentTimeMillis();
          long nowSecs = TimeUnit.MILLISECONDS.toSeconds(now);
          switch (newStatus) {
            case RUNNING:
              Map<String, String> runtimeArgs = GSON.fromJson(target.getProperties().get("runtimeArgs"),
                                                              STRING_MAP_TYPE);
              Map<String, String> systemArgs = GSON.fromJson(target.getProperties().get("systemArgs"),
                                                             STRING_MAP_TYPE);
              if (runtimeArgs == null) {
                runtimeArgs = EMPTY_STRING_MAP;
              }
              if (systemArgs == null) {
                systemArgs = EMPTY_STRING_MAP;
              }
              mds.recordProgramStart(id, pid, nowSecs, target.getTwillRunId(), runtimeArgs, systemArgs);
              break;
            case SUSPENDED:
              mds.recordProgramSuspend(id, pid);
              break;
            case COMPLETED:
            case KILLED:
            case FAILED:
              BasicThrowable failureCause = newStatus == ProgramRunStatus.FAILED
                ? new BasicThrowable(new Throwable("Marking run record as failed since no running program found."))
                : null;
              mds.recordProgramStop(id, pid, nowSecs, newStatus, failureCause);
              break;
            default:
              break;
          }
        }
        return null;
      }
    }, apps.get());
  }

  @Override
  public void setStart(final Id.Program id, final String pid, final long startTime,
                       final String twillRunId, final Map<String, String> runtimeArgs,
                       final Map<String, String> systemArgs) {
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.recordProgramStart(id, pid, startTime, twillRunId, runtimeArgs, systemArgs);
        return null;
      }
    }, apps.get());
  }

  @Override
  public void setStart(Id.Program id, String pid, long startTime) {
    setStart(id, pid, startTime, null, EMPTY_STRING_MAP, EMPTY_STRING_MAP);
  }

  @Override
  public void setStop(final Id.Program id, final String pid, final long endTime, final ProgramRunStatus runStatus) {
    setStop(id, pid, endTime, runStatus, null);
  }

  @Override
  public void setStop(final Id.Program id, final String pid, final long endTime, final ProgramRunStatus runStatus,
                      final BasicThrowable failureCause) {
    Preconditions.checkArgument(runStatus != null, "Run state of program run should be defined");
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.recordProgramStop(id, pid, endTime, runStatus, failureCause);
        return null;
      }
    }, apps.get());

    // This block has been added so that completed workflow runs can be logged to the workflow dataset
    if (id.getType() == ProgramType.WORKFLOW && runStatus == ProgramRunStatus.COMPLETED) {
      Id.Workflow workflow = Id.Workflow.from(id.getApplication(), id.getId());
      recordCompletedWorkflow(workflow, pid);
    }
    // todo: delete old history data
  }

  private void recordCompletedWorkflow(final Id.Workflow id, String pid) {
    final RunRecordMeta run = getRun(id, pid);
    if (run == null) {
      return;
    }
    Id.Application app = id.getApplication();
    ApplicationSpecification appSpec = getApplication(app);
    if (appSpec == null || appSpec.getWorkflows() == null || appSpec.getWorkflows().get(id.getId()) == null) {
      return;
    }

    boolean workFlowNodeFailed = false;
    WorkflowSpecification workflowSpec = appSpec.getWorkflows().get(id.getId());
    Map<String, WorkflowNode> nodeIdMap = workflowSpec.getNodeIdMap();
    final List<WorkflowDataset.ProgramRun> programRunsList = new ArrayList<>();
    for (Map.Entry<String, String> entry : run.getProperties().entrySet()) {
      if (!("workflowToken".equals(entry.getKey()) || "runtimeArgs".equals(entry.getKey())
        || "workflowNodeState".equals(entry.getKey()))) {
        WorkflowActionNode workflowNode = (WorkflowActionNode) nodeIdMap.get(entry.getKey());
        ProgramType programType = ProgramType.valueOfSchedulableType(workflowNode.getProgram().getProgramType());
        Id.Program innerProgram = Id.Program.from(app.getNamespaceId(), app.getId(), programType, entry.getKey());
        RunRecordMeta innerProgramRun = getRun(innerProgram, entry.getValue());
        if (innerProgramRun != null && innerProgramRun.getStatus().equals(ProgramRunStatus.COMPLETED)) {
          Long stopTs = innerProgramRun.getStopTs();
          // since the program is completed, the stop ts cannot be null
          Preconditions.checkState(stopTs != null, "Since the program has completed, expected its stop time to not " +
            "be null. Program = %s, Workflow = %s, Run = %s, Stop Ts = %s", innerProgram, id, run, stopTs);
          programRunsList.add(new WorkflowDataset.ProgramRun(
            entry.getKey(), entry.getValue(), programType, stopTs - innerProgramRun.getStartTs()));
        } else {
          workFlowNodeFailed = true;
          break;
        }
      }
    }

    if (workFlowNodeFailed) {
      return;
    }

    workflowsTx.get().executeUnchecked(new TransactionExecutor.Function<WorkflowDataset, Void>() {
      @Override
      public Void apply(WorkflowDataset dataset) {
        dataset.write(id, run, programRunsList);
        return null;
      }
    }, workflows.get());
  }

  @Override
  public void setSuspend(final Id.Program id, final String pid) {
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.recordProgramSuspend(id, pid);
        return null;
      }
    }, apps.get());
  }

  @Override
  public void setResume(final Id.Program id, final String pid) {
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.recordProgramResumed(id, pid);
        return null;
      }
    }, apps.get());
  }

  @Nullable
  public WorkflowStatistics getWorkflowStatistics(final Id.Workflow id, final long startTime,
                                                  final long endTime, final List<Double> percentiles) {
    return workflowsTx.get().executeUnchecked(
      new TransactionExecutor.Function<WorkflowDataset, WorkflowStatistics>() {
        @Override
        public WorkflowStatistics apply(WorkflowDataset dataset) throws Exception {
          return dataset.getStatistics(id, startTime, endTime, percentiles);
        }
      }, workflows.get());
  }

  @Override
  public WorkflowDataset.WorkflowRunRecord getWorkflowRun(final Id.Workflow workflowId, final String runId) {
    return workflowsTx.get().executeUnchecked(
      new TransactionExecutor.Function<WorkflowDataset, WorkflowDataset.WorkflowRunRecord>() {
        @Override
        public WorkflowDataset.WorkflowRunRecord apply(WorkflowDataset dataset) throws Exception {
          return dataset.getRecord(workflowId, runId);
        }
      }, workflows.get());
  }

  @Override
  public Collection<WorkflowDataset.WorkflowRunRecord> retrieveSpacedRecords(final Id.Workflow workflow,
                                                                             final String runId,
                                                                             final int limit,
                                                                             final long timeInterval) {
    return workflowsTx.get().executeUnchecked(
      new TransactionExecutor.Function<WorkflowDataset, Collection<WorkflowDataset.WorkflowRunRecord>>() {
        @Override
        public Collection<WorkflowDataset.WorkflowRunRecord> apply(WorkflowDataset dataset) throws Exception {
          return dataset.getDetailsOfRange(workflow, runId, limit, timeInterval);
        }
      }, workflows.get());
  }

  @Override
  public List<RunRecordMeta> getRuns(final Id.Program id, final ProgramRunStatus status,
                                     final long startTime, final long endTime, final int limit) {
    return getRuns(id, status, startTime, endTime, limit, null);
  }

  @Override
  public List<RunRecordMeta> getRuns(final Id.Program id, final ProgramRunStatus status,
                                     final long startTime, final long endTime, final int limit,
                                     @Nullable final Predicate<RunRecordMeta> filter) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, List<RunRecordMeta>>() {
        @Override
        public List<RunRecordMeta> apply(AppMetadataStore mds) throws Exception {
          return mds.getRuns(id, status, startTime, endTime, limit, filter);
        }
      }, apps.get());
  }

  @Override
  public List<RunRecordMeta> getRuns(final ProgramRunStatus status, final Predicate<RunRecordMeta> filter) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, List<RunRecordMeta>>() {
        @Override
        public List<RunRecordMeta> apply(AppMetadataStore mds) throws Exception {
          return mds.getRuns(status, filter);
        }
      }, apps.get());
  }

  /**
   * Returns run record for a given run.
   *
   * @param id program id
   * @param runid run id
   * @return run record for runid
   */
  @Override
  public RunRecordMeta getRun(final Id.Program id, final String runid) {
    return appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, RunRecordMeta>() {
      @Override
      public RunRecordMeta apply(AppMetadataStore mds) throws Exception {
        return mds.getRun(id, runid);
      }
    }, apps.get());
  }

  @Override
  public void addApplication(final Id.Application id, final ApplicationSpecification spec) {

    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.writeApplication(id.getNamespaceId(), id.getId(), spec);
        return null;
      }
    }, apps.get());

  }

  // todo: this method should be moved into DeletedProgramHandlerState, bad design otherwise
  @Override
  public List<ProgramSpecification> getDeletedProgramSpecifications(final Id.Application id,
                                                                    ApplicationSpecification appSpec) {

    ApplicationMeta existing = appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, ApplicationMeta>() {
        @Override
        public ApplicationMeta apply(AppMetadataStore mds) throws Exception {
          return mds.getApplication(id.getNamespaceId(), id.getId());
        }
      }, apps.get());

    List<ProgramSpecification> deletedProgramSpecs = Lists.newArrayList();

    if (existing != null) {
      ApplicationSpecification existingAppSpec = existing.getSpec();

      ImmutableMap<String, ProgramSpecification> existingSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(existingAppSpec.getMapReduce())
                                                                      .putAll(existingAppSpec.getSpark())
                                                                      .putAll(existingAppSpec.getWorkflows())
                                                                      .putAll(existingAppSpec.getFlows())
                                                                      .putAll(existingAppSpec.getServices())
                                                                      .putAll(existingAppSpec.getWorkers())
                                                                      .build();

      ImmutableMap<String, ProgramSpecification> newSpec = new ImmutableMap.Builder<String, ProgramSpecification>()
                                                                      .putAll(appSpec.getMapReduce())
                                                                      .putAll(appSpec.getSpark())
                                                                      .putAll(appSpec.getWorkflows())
                                                                      .putAll(appSpec.getFlows())
                                                                      .putAll(appSpec.getServices())
                                                                      .putAll(appSpec.getWorkers())
                                                                      .build();


      MapDifference<String, ProgramSpecification> mapDiff = Maps.difference(existingSpec, newSpec);
      deletedProgramSpecs.addAll(mapDiff.entriesOnlyOnLeft().values());
    }

    return deletedProgramSpecs;
  }

  @Override
  public void addStream(final Id.Namespace id, final StreamSpecification streamSpec) {
    appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
      @Override
      public Void apply(AppMetadataStore mds) throws Exception {
        mds.writeStream(id.getId(), streamSpec);
        return null;
      }
    }, apps.get());
  }

  @Override
  public StreamSpecification getStream(final Id.Namespace id, final String name) {
    return appsTx.get().executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, StreamSpecification>() {
      @Override
      public StreamSpecification apply(AppMetadataStore mds) throws Exception {
        return mds.getStream(id.getId(), name);
      }
    }, apps.get());
  }

  @Override
  public Collection<StreamSpecification> getAllStreams(final Id.Namespace id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Collection<StreamSpecification>>() {
        @Override
        public Collection<StreamSpecification> apply(AppMetadataStore mds) throws Exception {
          return mds.getAllStreams(id.getId());
        }
      }, apps.get());
  }

  @Override
  public FlowSpecification setFlowletInstances(final Id.Program id, final String flowletId, final int count) {
    Preconditions.checkArgument(count > 0, "cannot change number of flowlet instances to negative number: " + count);

    LOG.trace("Setting flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, " +
                "new instances count: {}", id.getNamespaceId(), id.getApplicationId(), id.getId(), flowletId, count);

    FlowSpecification flowSpec = appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, FlowSpecification>() {
        @Override
        public FlowSpecification apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          ApplicationSpecification newAppSpec = updateFlowletInstancesInAppSpec(appSpec, id, flowletId, count);
          mds.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
          return appSpec.getFlows().get(id.getId());
        }
      }, apps.get());

    LOG.trace("Set flowlet instances: namespace: {}, application: {}, flow: {}, flowlet: {}, instances now: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), flowletId, count);
    return flowSpec;
  }

  @Override
  public int getFlowletInstances(final Id.Program id, final String flowletId) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Integer>() {
        @Override
        public Integer apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
          FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);
          return flowletDef.getInstances();
        }
      }, apps.get());
  }

  @Override
  public void setWorkerInstances(final Id.Program id, final int instances) {
    Preconditions.checkArgument(instances > 0, "cannot change number of program " +
      "instances to negative number: " + instances);
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
          WorkerSpecification newSpecification = new WorkerSpecification(workerSpec.getClassName(),
                                                                         workerSpec.getName(),
                                                                         workerSpec.getDescription(),
                                                                         workerSpec.getProperties(),
                                                                         workerSpec.getDatasets(),
                                                                         workerSpec.getResources(),
                                                                         instances);
          ApplicationSpecification newAppSpec = replaceWorkerInAppSpec(appSpec, id, newSpecification);
          mds.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
          return null;
        }
      }, apps.get());

    LOG.trace("Setting program instances: namespace: {}, application: {}, worker: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), instances);
  }

  @Override
  public void setServiceInstances(final Id.Program id, final int instances) {
    Preconditions.checkArgument(instances > 0,
                                "cannot change number of program instances to negative number: %s", instances);

    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);

          // Create a new spec copy from the old one, except with updated instances number
          serviceSpec = new ServiceSpecification(serviceSpec.getClassName(), serviceSpec.getName(),
                                                 serviceSpec.getDescription(), serviceSpec.getHandlers(),
                                                 serviceSpec.getResources(), instances);

          ApplicationSpecification newAppSpec = replaceServiceSpec(appSpec, id.getId(), serviceSpec);
          mds.updateAppSpec(id.getNamespaceId(), id.getApplicationId(), newAppSpec);
          return null;
        }
      }, apps.get());

    LOG.trace("Setting program instances: namespace: {}, application: {}, service: {}, new instances count: {}",
              id.getNamespaceId(), id.getApplicationId(), id.getId(), instances);
  }

  @Override
  public int getServiceInstances(final Id.Program id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Integer>() {
        @Override
        public Integer apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          ServiceSpecification serviceSpec = getServiceSpecOrFail(id, appSpec);
          return serviceSpec.getInstances();
        }
      }, apps.get());
  }

  @Override
  public int getWorkerInstances(final Id.Program id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Integer>() {
        @Override
        public Integer apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, id);
          WorkerSpecification workerSpec = getWorkerSpecOrFail(id, appSpec);
          return workerSpec.getInstances();
        }
      }, apps.get());
  }

  @Override
  public void removeApplication(final Id.Application id) {
    LOG.trace("Removing application: namespace: {}, application: {}", id.getNamespaceId(), id.getId());

    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          mds.deleteApplication(id.getNamespaceId(), id.getId());
          mds.deleteProgramHistory(id.getNamespaceId(), id.getId());
          return null;
        }
      }, apps.get());
  }

  @Override
  public void removeAllApplications(final Id.Namespace id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getId());

    appsTx.get()
      .executeUnchecked(new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          mds.deleteApplications(id.getId());
          mds.deleteProgramHistory(id.getId());
          return null;
        }
      }, apps.get());
  }

  @Override
  public void removeAll(final Id.Namespace id) {
    LOG.trace("Removing all applications of namespace with id: {}", id.getId());

    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          mds.deleteApplications(id.getId());
          mds.deleteAllStreams(id.getId());
          mds.deleteProgramHistory(id.getId());
          return null;
        }
      }, apps.get());
  }

  @Override
  public Map<String, String> getRuntimeArguments(final Id.Run runId) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Map<String, String>>() {
        @Override
        public Map<String, String> apply(AppMetadataStore mds) throws Exception {
          RunRecordMeta runRecord = mds.getRun(runId.getProgram(), runId.getId());
          if (runRecord != null) {
            Map<String, String> properties = runRecord.getProperties();
            Map<String, String> runtimeArgs = GSON.fromJson(properties.get("runtimeArgs"), STRING_MAP_TYPE);
            if (runtimeArgs != null) {
              return runtimeArgs;
            }
            LOG.debug("Runtime arguments for program {}, run {} not found. Returning empty.",
                      runId.getProgram(), runId.getId());
          }
          return EMPTY_STRING_MAP;
        }
      }, apps.get());
  }

  @Nullable
  @Override
  public ApplicationSpecification getApplication(final Id.Application id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, ApplicationSpecification>() {
        @Override
        public ApplicationSpecification apply(AppMetadataStore mds) throws Exception {
          return getApplicationSpec(mds, id);
        }
      }, apps.get());
  }

  @Override
  public Collection<ApplicationSpecification> getAllApplications(final Id.Namespace id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Collection<ApplicationSpecification>>() {
        @Override
        public Collection<ApplicationSpecification> apply(AppMetadataStore mds) throws Exception {
          return Lists.transform(
            mds.getAllApplications(id.getId()),
            new Function<ApplicationMeta, ApplicationSpecification>() {
              @Override
              public ApplicationSpecification apply(ApplicationMeta input) {
                return input.getSpec();
              }
            });
        }
      }, apps.get());
  }

  @Override
  public void addSchedule(final Id.Program program, final ScheduleSpecification scheduleSpecification) {
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, program);
          Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
          String scheduleName = scheduleSpecification.getSchedule().getName();
          Preconditions.checkArgument(!schedules.containsKey(scheduleName), "Schedule with the name '" +
            scheduleName + "' already exists.");
          schedules.put(scheduleSpecification.getSchedule().getName(), scheduleSpecification);
          ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
          mds.updateAppSpec(program.getNamespaceId(), program.getApplicationId(), newAppSpec);
          return null;
        }
      }, apps.get());
  }

  @Override
  public void deleteSchedule(final Id.Program program, final String scheduleName) {
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getAppSpecOrFail(mds, program);
          Map<String, ScheduleSpecification> schedules = Maps.newHashMap(appSpec.getSchedules());
          ScheduleSpecification removed = schedules.remove(scheduleName);
          if (removed == null) {
            throw new NoSuchElementException("no such schedule @ account id: " + program.getNamespaceId() +
                                               ", app id: " + program.getApplication() +
                                               ", program id: " + program.getId() +
                                               ", schedule name: " + scheduleName);
          }

          ApplicationSpecification newAppSpec = new AppSpecificationWithChangedSchedules(appSpec, schedules);
          mds.updateAppSpec(program.getNamespaceId(), program.getApplicationId(), newAppSpec);
          return null;
        }
      }, apps.get());
  }

  private static class AppSpecificationWithChangedSchedules extends ForwardingApplicationSpecification {
    private final Map<String, ScheduleSpecification> newSchedules;

    private AppSpecificationWithChangedSchedules(ApplicationSpecification delegate,
                                                 Map<String, ScheduleSpecification> newSchedules) {
      super(delegate);
      this.newSchedules = newSchedules;
    }

    @Override
    public Map<String, ScheduleSpecification> getSchedules() {
      return newSchedules;
    }
  }

  @Override
  public boolean applicationExists(final Id.Application id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Boolean>() {
        @Override
        public Boolean apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getApplicationSpec(mds, id);
          return appSpec != null;
        }
      }, apps.get());
  }

  @Override
  public boolean programExists(final Id.Program id) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Boolean>() {
        @Override
        public Boolean apply(AppMetadataStore mds) throws Exception {
          ApplicationSpecification appSpec = getApplicationSpec(mds, id.getApplication());
          return appSpec != null && programExists(id, appSpec);
        }
      }, apps.get());
  }

  private boolean programExists(Id.Program id, ApplicationSpecification appSpec) {
    switch (id.getType()) {
      case FLOW:      return appSpec.getFlows().containsKey(id.getId());
      case MAPREDUCE: return appSpec.getMapReduce().containsKey(id.getId());
      case SERVICE:   return appSpec.getServices().containsKey(id.getId());
      case SPARK:     return appSpec.getSpark().containsKey(id.getId());
      case WEBAPP:    return false;
      case WORKER:    return appSpec.getWorkers().containsKey(id.getId());
      case WORKFLOW:  return appSpec.getWorkflows().containsKey(id.getId());
      default:        throw new IllegalArgumentException("Unexpected ProgramType " + id.getType());
    }
  }

  @Override
  public void updateWorkflowToken(final ProgramRunId workflowRunId, final WorkflowToken token) {
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          mds.updateWorkflowToken(workflowRunId, token);
          return null;
        }
      }, apps.get());
  }

  @Override
  public WorkflowToken getWorkflowToken(final Id.Workflow workflowId, final String workflowRunId) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, WorkflowToken>() {
        @Override
        public WorkflowToken apply(AppMetadataStore mds) throws Exception {
          return mds.getWorkflowToken(workflowId, workflowRunId);
        }
      }, apps.get());
  }

  @Override
  public void addWorkflowNodeState(final ProgramRunId workflowRunId, final WorkflowNodeStateDetail nodeStateDetail) {
    appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Void>() {
        @Override
        public Void apply(AppMetadataStore mds) throws Exception {
          mds.addWorkflowNodeState(workflowRunId, nodeStateDetail);
          return null;
        }
      }, apps.get());
  }

  @Override
  public List<WorkflowNodeStateDetail> getWorkflowNodeStates(final ProgramRunId workflowRunId) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, List<WorkflowNodeStateDetail>>() {
        @Override
        public List<WorkflowNodeStateDetail> apply(AppMetadataStore mds) throws Exception {
          return mds.getWorkflowNodeStates(workflowRunId);
        }
      }, apps.get());
  }

  @VisibleForTesting
  void clear() throws Exception {
    truncate(dsFramework.getAdmin(APP_META_INSTANCE_ID, null));
    truncate(dsFramework.getAdmin(WORKFLOW_STATS_INSTANCE_ID, null));
  }

  private void truncate(DatasetAdmin admin) throws Exception {
    if (admin != null) {
      admin.truncate();
    }
  }

  private ApplicationSpecification getApplicationSpec(AppMetadataStore mds, Id.Application id) {
    ApplicationMeta meta = mds.getApplication(id.getNamespaceId(), id.getId());
    return meta == null ? null : meta.getSpec();
  }

  private static ApplicationSpecification replaceServiceSpec(ApplicationSpecification appSpec,
                                                             String serviceName,
                                                             ServiceSpecification serviceSpecification) {
    return new ApplicationSpecificationWithChangedServices(appSpec, serviceName, serviceSpecification);
  }

  private static final class ApplicationSpecificationWithChangedServices extends ForwardingApplicationSpecification {
    private final String serviceName;
    private final ServiceSpecification serviceSpecification;

    private ApplicationSpecificationWithChangedServices(ApplicationSpecification delegate,
                                                        String serviceName, ServiceSpecification serviceSpecification) {
      super(delegate);
      this.serviceName = serviceName;
      this.serviceSpecification = serviceSpecification;
    }

    @Override
    public Map<String, ServiceSpecification> getServices() {
      Map<String, ServiceSpecification> services = Maps.newHashMap(super.getServices());
      services.put(serviceName, serviceSpecification);
      return services;
    }
  }

  private static FlowletDefinition getFlowletDefinitionOrFail(FlowSpecification flowSpec,
                                                              String flowletId, Id.Program id) {
    FlowletDefinition flowletDef = flowSpec.getFlowlets().get(flowletId);
    if (flowletDef == null) {
      throw new NoSuchElementException("no such flowlet @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId() +
                                           ", flowlet id: " + flowletId);
    }
    return flowletDef;
  }

  private static FlowSpecification getFlowSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    FlowSpecification flowSpec = appSpec.getFlows().get(id.getId());
    if (flowSpec == null) {
      throw new NoSuchElementException("no such flow @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", flow id: " + id.getId());
    }
    return flowSpec;
  }

  private static ServiceSpecification getServiceSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    ServiceSpecification spec = appSpec.getServices().get(id.getId());
    if (spec == null) {
      throw new NoSuchElementException("no such service @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication() +
                                           ", service id: " + id.getId());
    }
    return spec;
  }

  private static WorkerSpecification getWorkerSpecOrFail(Id.Program id, ApplicationSpecification appSpec) {
    WorkerSpecification workerSpecification = appSpec.getWorkers().get(id.getId());
    if (workerSpecification == null) {
      throw new NoSuchElementException("no such worker @ namespace id: " + id.getNamespaceId() +
                                         ", app id: " + id.getApplication() +
                                         ", worker id: " + id.getId());
    }
    return workerSpecification;
  }

  private static ApplicationSpecification updateFlowletInstancesInAppSpec(ApplicationSpecification appSpec,
                                                                          Id.Program id, String flowletId, int count) {

    FlowSpecification flowSpec = getFlowSpecOrFail(id, appSpec);
    FlowletDefinition flowletDef = getFlowletDefinitionOrFail(flowSpec, flowletId, id);

    final FlowletDefinition adjustedFlowletDef = new FlowletDefinition(flowletDef, count);
    return replaceFlowletInAppSpec(appSpec, id, flowSpec, adjustedFlowletDef);
  }

  private ApplicationSpecification getAppSpecOrFail(AppMetadataStore mds, Id.Program id) {
    ApplicationSpecification appSpec = getApplicationSpec(mds, id.getApplication());
    if (appSpec == null) {
      throw new NoSuchElementException("no such application @ namespace id: " + id.getNamespaceId() +
                                           ", app id: " + id.getApplication().getId());
    }
    return appSpec;
  }

  private static ApplicationSpecification replaceInAppSpec(final ApplicationSpecification appSpec,
                                                    final Id.Program id,
                                                    final FlowSpecification flowSpec,
                                                    final FlowletDefinition adjustedFlowletDef,
                                                    final List<FlowletConnection> connections) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id,
                                new FlowSpecificationWithChangedFlowletsAndConnections(flowSpec,
                                                                                       adjustedFlowletDef,
                                                                                       connections));
  }

  private static class FlowSpecificationWithChangedFlowlets extends ForwardingFlowSpecification {
    private final FlowletDefinition adjustedFlowletDef;

    private FlowSpecificationWithChangedFlowlets(FlowSpecification delegate,
                                                 FlowletDefinition adjustedFlowletDef) {
      super(delegate);
      this.adjustedFlowletDef = adjustedFlowletDef;
    }

    @Override
    public Map<String, FlowletDefinition> getFlowlets() {
      Map<String, FlowletDefinition> flowlets = Maps.newHashMap(super.getFlowlets());
      flowlets.put(adjustedFlowletDef.getFlowletSpec().getName(), adjustedFlowletDef);
      return flowlets;
    }
  }

  private static final class FlowSpecificationWithChangedFlowletsAndConnections
    extends FlowSpecificationWithChangedFlowlets {

    private final List<FlowletConnection> connections;

    private FlowSpecificationWithChangedFlowletsAndConnections(FlowSpecification delegate,
                                                               FlowletDefinition adjustedFlowletDef,
                                                               List<FlowletConnection> connections) {
      super(delegate, adjustedFlowletDef);
      this.connections = connections;
    }

    @Override
    public List<FlowletConnection> getConnections() {
      return connections;
    }
  }

  private static ApplicationSpecification replaceFlowletInAppSpec(final ApplicationSpecification appSpec,
                                                                  final Id.Program id,
                                                                  final FlowSpecification flowSpec,
                                                                  final FlowletDefinition adjustedFlowletDef) {
    // as app spec is immutable we have to do this trick
    return replaceFlowInAppSpec(appSpec, id, new FlowSpecificationWithChangedFlowlets(flowSpec, adjustedFlowletDef));
  }

  private static ApplicationSpecification replaceFlowInAppSpec(final ApplicationSpecification appSpec,
                                                               final Id.Program id,
                                                               final FlowSpecification newFlowSpec) {
    // as app spec is immutable we have to do this trick
    return new ApplicationSpecificationWithChangedFlows(appSpec, id.getId(), newFlowSpec);
  }

  private static final class ApplicationSpecificationWithChangedFlows extends ForwardingApplicationSpecification {
    private final FlowSpecification newFlowSpec;
    private final String flowId;

    private ApplicationSpecificationWithChangedFlows(ApplicationSpecification delegate,
                                                     String flowId, FlowSpecification newFlowSpec) {
      super(delegate);
      this.newFlowSpec = newFlowSpec;
      this.flowId = flowId;
    }

    @Override
    public Map<String, FlowSpecification> getFlows() {
      Map<String, FlowSpecification> flows = Maps.newHashMap(super.getFlows());
      flows.put(flowId, newFlowSpec);
      return flows;
    }
  }

  private static ApplicationSpecification replaceWorkerInAppSpec(final ApplicationSpecification appSpec,
                                                                 final Id.Program id,
                                                                 final WorkerSpecification workerSpecification) {
    return new ApplicationSpecificationWithChangedWorkers(appSpec, id.getId(), workerSpecification);
  }

  private static final class ApplicationSpecificationWithChangedWorkers extends ForwardingApplicationSpecification {
    private final String workerId;
    private final WorkerSpecification workerSpecification;

    private ApplicationSpecificationWithChangedWorkers(ApplicationSpecification delegate, String workerId,
                                                       WorkerSpecification workerSpec) {
      super(delegate);
      this.workerId = workerId;
      this.workerSpecification = workerSpec;
    }

    @Override
    public Map<String, WorkerSpecification> getWorkers() {
      Map<String, WorkerSpecification> workers = Maps.newHashMap(super.getWorkers());
      workers.put(workerId, workerSpecification);
      return workers;
    }
  }

  public Set<RunId> getRunningInRange(final long startTimeInSecs, final long endTimeInSecs) {
    return appsTx.get().executeUnchecked(
      new TransactionExecutor.Function<AppMetadataStore, Set<RunId>>() {
        @Override
        public Set<RunId> apply(AppMetadataStore mds) throws Exception {
          return mds.getRunningInRange(startTimeInSecs, endTimeInSecs);
        }
      }, apps.get());
  }
}
