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

package co.cask.cdap.security;

import co.cask.cdap.common.conf.CConfiguration;
import co.cask.cdap.common.conf.Constants;
import co.cask.cdap.common.kerberos.SecurityUtil;
import co.cask.cdap.common.namespace.NamespacedLocationFactory;
import co.cask.cdap.common.utils.DirUtils;
import co.cask.cdap.data2.security.CredentialsInfo;
import co.cask.cdap.data2.security.DelegationTokenRequester;
import co.cask.cdap.data2.security.ImpersonationInfo;
import co.cask.cdap.data2.security.ImpersonationUserResolver;
import co.cask.cdap.data2.security.ImpersonationUtils;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedId;
import com.google.common.base.Throwables;
import com.google.inject.Inject;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.twill.api.SecureStore;
import org.apache.twill.filesystem.Location;
import org.apache.twill.filesystem.LocationFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.concurrent.Callable;

/**
 *
 */
public class DefaultDelegationTokenRequester implements DelegationTokenRequester {

  private static final Logger LOG = LoggerFactory.getLogger(DefaultDelegationTokenRequester.class);

  private final CConfiguration cConf;
  private final LocationFactory locationFactory;
  private final NamespacedLocationFactory namespacedLocationFactory;
  private final ImpersonationUserResolver impersonationUserResolver;
  private final TokenSecureStoreUpdater tokenSecureStoreUpdater;

  @Inject
  public DefaultDelegationTokenRequester(CConfiguration cConf, LocationFactory locationFactory,
                                         NamespacedLocationFactory namespacedLocationFactory,
                                         ImpersonationUserResolver impersonationUserResolver,
                                         TokenSecureStoreUpdater tokenSecureStoreUpdater) {
    // TODO: only do this stuff in distributed CDAP
    // TODO: make sure other impersonation stuff only happens in distributed
    this.cConf = cConf;
    this.locationFactory = locationFactory;
    this.namespacedLocationFactory = namespacedLocationFactory;
    this.impersonationUserResolver = impersonationUserResolver;
    this.tokenSecureStoreUpdater = tokenSecureStoreUpdater;
  }

  /**
   * Resolves the UGI for a given namespace, acquires the delegation tokens for that UGI,
   * using {@link TokenSecureStoreUpdater}, and serializes these Credentials to a location.
   *
   * @return the location to which the credentials were serialized to.
   */
  @Override
  public CredentialsInfo getCredentials(NamespaceId namespaceId) throws IOException {
    // hdfs:///cdap/namespaces/foo/credentials
    Location location = namespacedLocationFactory.get(namespaceId.toId(), "credentials");
    // the getTempFile() doesn't create the file within the directory that you call it on. It simply appends the path
    // without a separator, which is why we manually append the "tmp"

    // hdfs:///cdap/namespaces/foo/credentials/tmp.5960fe60-6fd8-4f3e-8e92-3fb6d4726006.credentials
    Location credentialsFile = location.append("tmp").getTempFile(".credentials");

    UserGroupInformation ugi = getResolvedUser(namespaceId);

    Credentials credentials;
    try {
      credentials = ImpersonationUtils.doAs(ugi, new Callable<Credentials>() {
        @Override
        public Credentials call() throws Exception {
          SecureStore update = tokenSecureStoreUpdater.update(null, null);
          return update.getStore();
        }
      });
    } catch (Exception e) {
      // move this wrapping logic into impersonator?
      throw Throwables.propagate(e);
    }
    try (DataOutputStream os = new DataOutputStream(new BufferedOutputStream(credentialsFile.getOutputStream("600")))) {
      credentials.writeTokenStorageToStream(os);
    }
    LOG.info("Wrote credentials for user {} to {}", ugi.getUserName(), credentialsFile);
    return new CredentialsInfo(credentialsFile.toURI().toString(), ugi.getShortUserName());
  }


  /**
   * Resolves the impersonation info for a given namespace. Then, creates and returns a UserGroupInformation with this
   * information, performing any keytab localization, if necessary.
   *
   * @return a {@link UserGroupInformation}, based upon the information configured for a particular namespace
   * @throws IOException if there was any IOException during localization of the keytab
   */
  // TODO: move this into IUserResolver and use from ADProgramRunner?
  public UserGroupInformation getResolvedUser(NamespacedId namespacedId) throws IOException {
    ImpersonationInfo impersonationInfo = impersonationUserResolver.getImpersonationInfo(namespacedId);
    // TODO: figure out what this is (where exactly it writes to)
    File tempDir = DirUtils.createTempDir(new File(cConf.get(Constants.CFG_LOCAL_DATA_DIR),
                                                   cConf.get(Constants.AppFabric.TEMP_DIR)).getAbsoluteFile());

    String localizedKeytabPath =
      ImpersonationUtils.localizeKeytab(locationFactory, URI.create(impersonationInfo.getKeytabPath()), tempDir);

    LOG.info("Configured impersonation info: {}", impersonationInfo);

    String expandedPrincipal = SecurityUtil.expandPrincipal(impersonationInfo.getPrincipal());
    LOG.info("Logging in as: principal={}, keytab={}", expandedPrincipal, localizedKeytabPath);

    return UserGroupInformation.loginUserFromKeytabAndReturnUGI(expandedPrincipal, localizedKeytabPath);
  }
}
