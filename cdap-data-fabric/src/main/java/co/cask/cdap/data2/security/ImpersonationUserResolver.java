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
import co.cask.cdap.proto.NamespaceConfig;
import co.cask.cdap.proto.NamespaceMeta;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.proto.id.NamespacedId;
import co.cask.cdap.store.NamespaceStore;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.inject.Inject;

/**
 * Helper class to resolve the principal which CDAP will launch programs as.
 * This class should only be used if Kerberos security is enabled and it is for a user namespace.
 */
public class ImpersonationUserResolver {

  private final NamespaceStore namespaceStore;
  private final String defaultPrincipal;
  private final String defaultKeytabPath;

  @Inject
  public ImpersonationUserResolver(NamespaceStore namespaceStore, CConfiguration cConf) {
    this.namespaceStore = namespaceStore;
    this.defaultPrincipal = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_PRINCIPAL);
    this.defaultKeytabPath = cConf.get(Constants.Security.CFG_CDAP_MASTER_KRB_KEYTAB_PATH);
  }

  /**
   * Get impersonation info for a given namespace. If the info configured at the namespace level is empty,
   * returns the info configured at the cdap level.
   *
   * @return configured {@link ImpersonationInfo}.
   */
  public ImpersonationInfo getImpersonationInfo(NamespacedId namespacedId) {
    NamespaceMeta meta = namespaceStore.get(new NamespaceId(namespacedId.getNamespace()).toId());
    Preconditions.checkNotNull(meta,
                               "Failed to retrieve namespace meta for namespace id %s", namespacedId.getNamespace());
    NamespaceConfig namespaceConfig = meta.getConfig();

    String principal = Objects.firstNonNull(namespaceConfig.getPrincipal(), defaultPrincipal);
    String keytabPath = Objects.firstNonNull(namespaceConfig.getKeytabPath(), defaultKeytabPath);
    return new ImpersonationInfo(principal, keytabPath);
  }
}
