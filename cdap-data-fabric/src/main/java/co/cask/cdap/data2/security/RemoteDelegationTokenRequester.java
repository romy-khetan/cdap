/*
 * Copyright © 2016 Cask Data, Inc.
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

package co.cask.cdap.data2.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.discovery.EndpointStrategy;
import co.cask.cdap.common.discovery.RandomEndpointStrategy;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.common.http.HttpRequest;
import co.cask.common.http.HttpRequestConfig;
import co.cask.common.http.HttpRequests;
import co.cask.common.http.HttpResponse;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.gson.Gson;
import com.google.inject.Inject;
import org.apache.twill.discovery.Discoverable;
import org.apache.twill.discovery.DiscoveryServiceClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class RemoteDelegationTokenRequester implements DelegationTokenRequester {

  private static final Logger LOG = LoggerFactory.getLogger(RemoteDelegationTokenRequester.class);
  private static final Gson GSON = new Gson();

  private final Supplier<EndpointStrategy> endpointStrategySupplier;
  private final HttpRequestConfig httpRequestConfig;

  @Inject
  RemoteDelegationTokenRequester(CConfiguration cConf, final DiscoveryServiceClient discoveryClient) {
    this.endpointStrategySupplier = Suppliers.memoize(new Supplier<EndpointStrategy>() {
      @Override
      public EndpointStrategy get() {
        return new RandomEndpointStrategy(discoveryClient.discover(Constants.Service.APP_FABRIC_HTTP));
      }
    });

    int httpClientTimeoutMs = cConf.getInt(Constants.HTTP_CLIENT_TIMEOUT_MS);
    this.httpRequestConfig = new HttpRequestConfig(httpClientTimeoutMs, httpClientTimeoutMs);
  }

  private String resolve(String resource) {
    Discoverable discoverable = endpointStrategySupplier.get().pick(3L, TimeUnit.SECONDS);
    if (discoverable == null) {
      throw new RuntimeException(
        String.format("Cannot discover service %s", Constants.Service.APP_FABRIC_HTTP));
    }
    InetSocketAddress addr = discoverable.getSocketAddress();

    return String.format("http://%s:%s%s/%s",
                         addr.getHostName(), addr.getPort(), "/v1", resource);
  }

  HttpResponse executeRequest(NamespaceId namespaceId) {
    String resolvedUrl = resolve(String.format("/namespaces/%s/credentials", namespaceId.getNamespace()));
    try {
      URL url = new URL(resolvedUrl);
      HttpRequest.Builder builder = HttpRequest.post(url);
      HttpResponse response = HttpRequests.execute(builder.build(), httpRequestConfig);
      if (response.getResponseCode() == HttpURLConnection.HTTP_OK) {
        return response;
      }
      throw new RuntimeException(String.format("%s Response: %s.",
                                               createErrorMessage(resolvedUrl),
                                               response));
    } catch (IOException e) {
      throw new RuntimeException(createErrorMessage(resolvedUrl),
                                 e);
    }
  }

  // creates error message, encoding details about the request
  private static String createErrorMessage(String resolvedUrl) {
    return String.format("Error making request to AppFabric Service at %s.", resolvedUrl);
  }

  @Override
  public CredentialsInfo getCredentials(NamespaceId namespaceId) {
    HttpResponse response = executeRequest(namespaceId);
    LOG.info("Received response: {}", response.getResponseBodyAsString());
    return GSON.fromJson(response.getResponseBodyAsString(), CredentialsInfo.class);
  }
}
