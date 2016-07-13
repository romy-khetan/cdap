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
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.proto.id.NamespaceId;
import com.google.inject.Inject;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

/**
 *
 */
public class Impersonator {

  private static final Logger LOG = LoggerFactory.getLogger(Impersonator.class);

  private final boolean kerberosEnabled;
  private final DelegationTokenRequester tokenRequester;
  private final LocationFactory locationFactory;

  @Inject
  // TODO: bind remote vs default
  // even when bound to default, it will write and read credentials off of HDFS
  public Impersonator(CConfiguration cConf, RemoteDelegationTokenRequester tokenRequester,
                      LocationFactory locationFactory) {
    this.kerberosEnabled = SecurityUtil.isKerberosEnabled(cConf);
    this.tokenRequester = tokenRequester;
    this.locationFactory = locationFactory;
  }

  public <T> T doAs(NamespaceId namespaceId, final Callable<T> callable) throws Exception {
    if (!kerberosEnabled) {
      return callable.call();
    }

    CredentialsInfo credentialsInfo = tokenRequester.getCredentials(namespaceId);

    // TODO: does the username matter? Or only the delegation tokens?
    UserGroupInformation impersonatedUGI = UserGroupInformation.createRemoteUser(credentialsInfo.getUser());

    Location location = locationFactory.create(URI.create(credentialsInfo.getUri()));
    Credentials credentials = readCredentials(location);
    impersonatedUGI.addCredentials(credentials);
    return ImpersonationUtils.doAs(impersonatedUGI, callable);
  }

  private static Credentials readCredentials(Location location) throws IOException {
    Credentials credentials = new Credentials();
    try (DataInputStream input = new DataInputStream(new BufferedInputStream(location.getInputStream()))) {
      credentials.readTokenStorageStream(input);
    }
    LOG.info("Read credentials from {}", location);
    return credentials;
  }
}
