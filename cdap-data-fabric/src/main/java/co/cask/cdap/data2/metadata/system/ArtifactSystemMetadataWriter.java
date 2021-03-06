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

package co.cask.cdap.data2.metadata.system;

import co.cask.cdap.api.plugin.PluginClass;
import co.cask.cdap.data2.metadata.store.MetadataStore;
import co.cask.cdap.proto.Id;
import co.cask.cdap.proto.artifact.ArtifactClasses;
import co.cask.cdap.proto.artifact.ArtifactInfo;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

/**
 * A {@link AbstractSystemMetadataWriter} for an {@link Id.Artifact artifact}.
 */
public class ArtifactSystemMetadataWriter extends AbstractSystemMetadataWriter {

  private final ArtifactInfo artifactInfo;

  public ArtifactSystemMetadataWriter(MetadataStore metadataStore, Id.Artifact artifactId,
                                      ArtifactInfo artifactInfo) {
    super(metadataStore, artifactId);
    this.artifactInfo = artifactInfo;
  }

  @Override
  protected Map<String, String> getSystemPropertiesToAdd() {
    ImmutableMap.Builder<String, String> properties = ImmutableMap.builder();
    ArtifactClasses classes = artifactInfo.getClasses();
    for (PluginClass pluginClass : classes.getPlugins()) {
      addPlugin(pluginClass, artifactInfo.getVersion(), properties);
    }
    return properties.build();
  }

  @Override
  protected String[] getSystemTagsToAdd() {
    return new String[] {
      artifactInfo.getName()
    };
  }
}
