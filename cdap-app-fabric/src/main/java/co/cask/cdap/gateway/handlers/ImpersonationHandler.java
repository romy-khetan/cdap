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

package co.cask.cdap.gateway.handlers;

import co.cask.cdap.data2.security.CredentialsInfo;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.security.DefaultDelegationTokenRequester;
import co.cask.http.AbstractHttpHandler;
import co.cask.http.HttpResponder;
import com.google.inject.Inject;
import org.jboss.netty.handler.codec.http.HttpRequest;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;

/**
 *
 */
// we don't share the same version as other handlers, so we can upgrade/iterate faster
@Path("/v1/namespaces/{namespace-id}")
public class ImpersonationHandler extends AbstractHttpHandler {

  private final DefaultDelegationTokenRequester tokenRequester;

  @Inject
  public ImpersonationHandler(DefaultDelegationTokenRequester tokenRequester) {
    // TODO: only do this stuff in distributed CDAP
    // TODO: make sure other impersonation stuff only happens in distributed
    this.tokenRequester = tokenRequester;
  }

  @POST
  @Path("/credentials")
  public void getCredentials(HttpRequest request, HttpResponder responder,
                             @PathParam("namespace-id") String namespaceId) throws Exception {
    CredentialsInfo location = tokenRequester.getCredentials(new NamespaceId(namespaceId));
    responder.sendJson(HttpResponseStatus.OK, location);
  }
}
