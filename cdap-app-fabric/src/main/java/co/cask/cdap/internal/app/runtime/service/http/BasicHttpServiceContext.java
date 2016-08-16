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

package co.cask.cdap.internal.app.runtime.service.http;

import co.cask.cdap.api.metrics.MetricsCollectionService;
import co.cask.cdap.api.security.store.SecureStore;
import co.cask.cdap.api.security.store.SecureStoreManager;
import co.cask.cdap.api.service.http.HttpServiceHandlerSpecification;
import co.cask.cdap.app.program.Program;
import co.cask.cdap.app.runtime.ProgramOptions;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.data2.dataset2.DatasetFramework;
import co.cask.cdap.internal.app.runtime.AbstractContext;
import co.cask.cdap.internal.app.runtime.plugin.PluginInstantiator;
import org.apache.tephra.TransactionContext;
import org.apache.tephra.TransactionSystemClient;
import org.apache.twill.discovery.DiscoveryServiceClient;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nullable;

/**
 * Default implementation of HttpServiceContext which simply stores and retrieves the
 * spec provided when this class is instantiated
 */
public class BasicHttpServiceContext extends AbstractContext implements TransactionalHttpServiceContext {

  private final HttpServiceHandlerSpecification spec;
  private final int instanceId;
  private final AtomicInteger instanceCount;

  /**
   * Creates a BasicHttpServiceContext for the given HttpServiceHandlerSpecification.
   * @param program program of the context.
   * @param programOptions program options for the program execution context
   * @param spec spec of the service handler of this context. If {@code null} is provided, this context
   *             is not associated with any service handler (e.g. for the http server itself).
   * @param instanceId instanceId of the component.
   * @param instanceCount total number of instances of the component.
   * @param metricsCollectionService metricsCollectionService to use for emitting metrics.
   * @param dsFramework dsFramework to use for getting datasets.
   * @param discoveryServiceClient discoveryServiceClient used to do service discovery.
   * @param txClient txClient to do transaction operations.
   * @param pluginInstantiator {@link PluginInstantiator}
   * @param secureStore
   */
  public BasicHttpServiceContext(Program program, ProgramOptions programOptions,
                                 @Nullable HttpServiceHandlerSpecification spec,
                                 int instanceId, AtomicInteger instanceCount,
                                 MetricsCollectionService metricsCollectionService,
                                 DatasetFramework dsFramework, DiscoveryServiceClient discoveryServiceClient,
                                 TransactionSystemClient txClient, @Nullable PluginInstantiator pluginInstantiator,
                                 SecureStore secureStore, SecureStoreManager secureStoreManager) {
    super(program, programOptions, spec == null ? Collections.<String>emptySet() : spec.getDatasets(),
          dsFramework, txClient, discoveryServiceClient, false,
          metricsCollectionService, createMetricsTags(spec, instanceId),
          secureStore, secureStoreManager, pluginInstantiator);
    this.spec = spec;
    this.instanceId = instanceId;
    this.instanceCount = instanceCount;
  }

  private static Map<String, String> createMetricsTags(@Nullable HttpServiceHandlerSpecification spec,
                                                       int instanceId) {
    Map<String, String> tags = new HashMap<>();
    tags.put(Constants.Metrics.Tag.INSTANCE_ID, String.valueOf(instanceId));
    if (spec != null) {
      tags.put(Constants.Metrics.Tag.HANDLER, spec.getName());
    }
    return tags;
  }

  /**
   * @return the {@link HttpServiceHandlerSpecification} for this context or {@code null} if there is no service
   *         handler associated with this context.
   */
  @Nullable
  @Override
  public HttpServiceHandlerSpecification getSpecification() {
    return spec;
  }

  @Override
  public int getInstanceCount() {
    return instanceCount.get();
  }

  @Override
  public int getInstanceId() {
    return instanceId;
  }

  @Override
  public TransactionContext newTransactionContext() {
    return getDatasetCache().newTransactionContext();
  }

  @Override
  public void dismissTransactionContext() {
    getDatasetCache().dismissTransactionContext();
  }
}
