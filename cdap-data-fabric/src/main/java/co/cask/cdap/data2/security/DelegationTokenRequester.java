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

import co.cask.cdap.proto.id.NamespaceId;

import java.io.IOException;

/**
 * Facilitates getting Credentials for the user configured for a given namespace.
 */
public interface DelegationTokenRequester {

  /**
   * Looks up the Kerberos principal to be impersonated for a given namespace and returns a {@link CredentialsInfo}
   * for that principal's credentials.
   *
   * @param namespaceId the specified namespace
   * @return the {@link CredentialsInfo} for the configured user
   */
  CredentialsInfo getCredentials(NamespaceId namespaceId) throws IOException;
}
