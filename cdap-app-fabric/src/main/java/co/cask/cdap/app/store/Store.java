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

package co.cask.cdap.app.store;

import co.cask.cdap.api.ProgramSpecification;
import co.cask.cdap.api.app.Application;
import co.cask.cdap.api.app.ApplicationSpecification;
import co.cask.cdap.api.data.stream.StreamSpecification;
import co.cask.cdap.api.flow.FlowSpecification;
import co.cask.cdap.api.schedule.ScheduleSpecification;
import co.cask.cdap.api.worker.Worker;
import co.cask.cdap.api.workflow.Workflow;
import co.cask.cdap.api.workflow.WorkflowToken;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.program.ProgramDescriptor;
import co.cask.cdap.common.ApplicationNotFoundException;
import co.cask.cdap.common.ProgramNotFoundException;
import co.cask.cdap.internal.app.store.RunRecordMeta;
import co.cask.cdap.internal.app.store.WorkflowDataset;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.WorkflowNodeStateDetail;
import co.cask.cdap.proto.WorkflowStatistics;
import co.cask.cdap.proto.id.ProgramRunId;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import org.apache.twill.api.RunId;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * Responsible for managing {@link Program} and {@link Application} metadata.
 */
public interface Store extends RuntimeStore {

  /**
   * Loads a given program.
   *
   * @param program id of the program
   * @return An instance of {@link ProgramDescriptor} if found.
   * @throws IOException
   */
  ProgramDescriptor loadProgram(Id.Program program) throws IOException, ApplicationNotFoundException,
                                                           ProgramNotFoundException;

  /**
   * Logs start of program run. This is a convenience method for testing, actual run starts should be recorded using
   * {@link #setStart(Id.Program, String, long, String, Map, Map)}.
   *
   * @param id        id of the program
   * @param pid       run id
   * @param startTime start timestamp in seconds; if run id is time-based pass the time from the run id
   */
  @VisibleForTesting
  void setStart(Id.Program id, String pid, long startTime);

  /**
   * Fetches run records for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        id of the program
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limit     max number of entries to fetch for this history call
   * @return          list of logged runs
   */
  List<RunRecordMeta> getRuns(Id.Program id, ProgramRunStatus status, long startTime, long endTime, int limit);

  /**
   * Fetches run records for particular program. Returns only finished runs.
   * Returned ProgramRunRecords are sorted by their startTime.
   *
   * @param id        id of the program
   * @param status    status of the program running/completed/failed or all
   * @param startTime fetch run history that has started after the startTime in seconds
   * @param endTime   fetch run history that has started before the endTime in seconds
   * @param limit     max number of entries to fetch for this history call
   * @param filter    predicate to be passed to filter the records
   * @return          list of logged runs
   */
  List<RunRecordMeta> getRuns(Id.Program id, ProgramRunStatus status, long startTime, long endTime, int limit,
                              Predicate<RunRecordMeta> filter);

  /**
   * Fetches the run records for the particular status.
   * @param status  status of the program running/completed/failed or all
   * @param filter  predicate to be passed to filter the records
   * @return        list of logged runs
   */
  List<RunRecordMeta> getRuns(ProgramRunStatus status, Predicate<RunRecordMeta> filter);

  /**
   * Fetches the run record for particular run of a program.
   *
   * @param id        id of the program
   * @param runid     run id of the program
   * @return          run record for the specified program and runid, null if not found
   */
  @Nullable
  RunRecordMeta getRun(Id.Program id, String runid);

  /**
   * Creates a new stream if it does not exist.
   * @param id the namespace id
   * @param stream the stream to create
   */
  void addStream(Id.Namespace id, StreamSpecification stream);

  /**
   * Get the spec of a named stream.
   * @param id the namespace id
   * @param name the name of the stream
   */
  StreamSpecification getStream(Id.Namespace id, String name);

  /**
   * Get the specs of all streams for a namespace.
   *
   * @param id the namespace id
   */
  Collection<StreamSpecification> getAllStreams(Id.Namespace id);

  /**
   * Creates new application if it doesn't exist. Updates existing one otherwise.
   *
   * @param id            application id
   * @param specification application specification to store
   */
  void addApplication(Id.Application id, ApplicationSpecification specification);


  /**
   * Return a list of program specifications that are deleted comparing the specification in the store with the
   * spec that is passed.
   *
   * @param id                   ApplicationId
   * @param specification        Application specification
   * @return                     List of ProgramSpecifications that are deleted
   */
  List<ProgramSpecification> getDeletedProgramSpecifications(Id.Application id,
                                                             ApplicationSpecification specification);

  /**
   * Returns application specification by id.
   *
   * @param id application id
   * @return application specification
   */
  @Nullable
  ApplicationSpecification getApplication(Id.Application id);

  /**
   * Returns a collection of all application specs in the specified namespace
   *
   * @param id the namespace to get application specs from
   * @return collection of all application specs in the namespace
   */
  Collection<ApplicationSpecification> getAllApplications(Id.Namespace id);

  /**
   * Sets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   * @param count new number of instances
   * @return The {@link FlowSpecification} before the instance change
   */
  FlowSpecification setFlowletInstances(Id.Program id, String flowletId, int count);

  /**
   * Gets number of instances of specific flowlet.
   *
   * @param id flow id
   * @param flowletId flowlet id
   */
  int getFlowletInstances(Id.Program id, String flowletId);

  /**
   * Sets the number of instances of a service.
   *
   * @param id id of the program
   * @param instances number of instances
   */
  void setServiceInstances(Id.Program id, int instances);

  /**
   * Returns the number of instances of a service.
   * @param id id of the program
   * @return number of instances
   */
  int getServiceInstances(Id.Program id);

  /**
   * Sets the number of instances of a {@link Worker}
   *
   * @param id id of the program
   * @param instances number of instances
   */
  void setWorkerInstances(Id.Program id, int instances);

  /**
   * Gets the number of instances of a {@link Worker}
   *
   * @param id id of the program
   * @return number of instances
   */
  int getWorkerInstances(Id.Program id);

  /**
   * Removes all program under the given application and also the application itself.
   *
   * @param id Application id
   */
  void removeApplication(Id.Application id);

  /**
   * Removes all applications (with programs) associated with the given namespace.
   *
   * @param id namespace id whose applications to remove
   */
  void removeAllApplications(Id.Namespace id);

  /**
   * Remove all metadata associated with the given namespace.
   *
   * @param id namespace id whose items to remove
   */
  void removeAll(Id.Namespace id);

  /**
   * Get run time arguments for a program.
   *
   * @param runId id of the program run
   * @return Map of key, value pairs
   */
  Map<String, String> getRuntimeArguments(Id.Run runId);

  /**
   * Adds a schedule for a particular program. If the schedule with the name already exists, the method will
   * throw RuntimeException.
   * @param program defines program to which a schedule is being added
   * @param scheduleSpecification defines the schedule to be added for the program
   */
  void addSchedule(Id.Program program, ScheduleSpecification scheduleSpecification);

  /**
   * Deletes data for an application from the WorkflowDataset table
   * @param id id of application to be deleted
   */
  void deleteWorkflowStats(final Id.Application id);

  /**
   * Deletes a schedules from a particular program
   * @param program defines program from which a schedule is being deleted
   * @param scheduleName the name of the schedule to be removed from the program
   */
  void deleteSchedule(Id.Program program, String scheduleName);

  /**
   * Check if an application exists.
   * @param id id of application.
   * @return true if the application exists, false otherwise.
   */
  boolean applicationExists(Id.Application id);

  /**
   * Check if a program exists.
   * @param id id of the program
   * @return true if the program exists, false otherwise.
   */
  boolean programExists(Id.Program id);

  /**
   * Retrieves the {@link WorkflowToken} for a specified run of a workflow.
   *
   * @param workflowId {@link Id.Workflow} of the workflow whose {@link WorkflowToken} is to be retrieved
   * @param workflowRunId Run Id of the workflow for which the {@link WorkflowToken} is to be retrieved
   * @return the {@link WorkflowToken} for the specified workflow run
   */
  WorkflowToken getWorkflowToken(Id.Workflow workflowId, String workflowRunId);

  /**
   * Get the node states for a given {@link Workflow} run.
   *
   * @param workflowRunId run of the Workflow.
   * @return {@link List} of {@link WorkflowNodeStateDetail}
   */
  List<WorkflowNodeStateDetail> getWorkflowNodeStates(ProgramRunId workflowRunId);

  /**
   * Used by {@link co.cask.cdap.gateway.handlers.WorkflowStatsSLAHttpHandler} to get the statistics of all completed
   * workflows in a time range.
   *
   * @param workflowId Workflow that needs to have its statistics returned
   * @param startTime StartTime of the range
   * @param endTime EndTime of the range
   * @param percentiles List of percentiles that the user wants to see
   * @return the statistics for a given workflow
   */
  WorkflowStatistics getWorkflowStatistics(Id.Workflow workflowId, long startTime,
                                           long endTime, List<Double> percentiles);

  /**
   * Returns the record that represents the run of a workflow.
   *
   * @param workflowId The Workflow whose run needs to be queried
   * @param runId RunId of the workflow run
   * @return A workflow run record corresponding to the runId
   */
  WorkflowDataset.WorkflowRunRecord getWorkflowRun(Id.Workflow workflowId, String runId);

  /**
   * Get a list of workflow runs that are spaced apart by time interval in both directions from the run id provided.
   *
   * @param workflow The workflow whose statistics need to be obtained
   * @param runId The run id of the workflow
   * @param limit The number of the records that the user wants to compare against on either side of the run
   * @param timeInterval The timeInterval with which the user wants to space out the runs
   * @return Map of runId of Workflow to DetailedStatistics of the run
   */
  Collection<WorkflowDataset.WorkflowRunRecord> retrieveSpacedRecords(Id.Workflow workflow, String runId,
                                                                      int limit, long timeInterval);

  /**
   * @return programs that were running between given start and end time.
   */
  Set<RunId> getRunningInRange(long startTimeInSecs, long endTimeInSecs);
}
