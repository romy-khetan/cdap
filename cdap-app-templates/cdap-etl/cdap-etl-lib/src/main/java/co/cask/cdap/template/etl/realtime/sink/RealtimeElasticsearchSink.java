/*
 * Copyright © 2015 Cask Data, Inc.
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

package co.cask.cdap.template.etl.realtime.sink;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.templates.plugins.PluginConfig;
import co.cask.cdap.template.etl.api.realtime.DataWriter;
import co.cask.cdap.template.etl.api.realtime.RealtimeContext;
import co.cask.cdap.template.etl.api.realtime.RealtimeSink;
import co.cask.cdap.template.etl.common.Properties;
import co.cask.cdap.template.etl.common.StructuredRecordStringConverter;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

/**
 * Realtime sink for elasticsearch.
 */
@Plugin(type = "sink")
@Name("Elasticsearch")
@Description("Real-time sink that outputs to the Elasticsearch server.")
public class RealtimeElasticsearchSink extends RealtimeSink<StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(RealtimeElasticsearchSink.class);
  private static final String INDEX_DESCRIPTION = "The name of the index where the data will be stored. " +
    "If the index does not already exist, it will be created using Elasticsearch's default properties.";
  private static final String TYPE_DESCRIPTION = "The name of the type where the data will be stored. " +
    "If it does not already exist, it will be created.";
  private static final String ID_DESCRIPTION = "The field that will determine the id for the document. " +
    "It should match a field name in the structured record of the input.";
  private static final String TRANSPORT_ADDRESS_DESCRIPTION = "The addresses for nodes. " +
    "Specify the address for at least one node, and separate others by commas. Other nodes will be sniffed out. " +
    "For example: host1:9300,host2:9300.";
  private static final String CLUSTER_DESCRIPTION = "The name of the cluster to connect to. " +
    "Defaults to \'elasticsearch\'.";

  private final RealtimeESSinkConfig realtimeESSinkConfig;
  private TransportClient client;

  public RealtimeElasticsearchSink(RealtimeESSinkConfig realtimeESSinkConfig) {
    this.realtimeESSinkConfig = realtimeESSinkConfig;
  }

  @Override
  public void initialize(RealtimeContext context) {
    realtimeESSinkConfig.cluster = Strings.isNullOrEmpty(realtimeESSinkConfig.cluster) ?
      "elasticsearch" : realtimeESSinkConfig.cluster;

    Settings settings = ImmutableSettings.settingsBuilder()
      .put("node.name", "cdap")
      .put("cluster.name", realtimeESSinkConfig.cluster)
      .put("client.transport.sniff", true).build();
    client = new TransportClient(settings);

    for (String address : realtimeESSinkConfig.transportAddresses.split(",")) {
      client.addTransportAddress(new InetSocketTransportAddress(address.split(":")[0],
                                                                Integer.valueOf(address.split(":")[1])));
    }
  }

  @Override
  public int write(Iterable<StructuredRecord> structuredRecords, DataWriter dataWriter) throws Exception {
    int numRecordsWritten = 0;

    BulkRequestBuilder bulkRequest = client.prepareBulk();
    for (StructuredRecord structuredRecord : structuredRecords) {
      if (Strings.isNullOrEmpty(realtimeESSinkConfig.idField)) {
        bulkRequest.add(client.prepareIndex(realtimeESSinkConfig.index, realtimeESSinkConfig.type)
          .setSource(StructuredRecordStringConverter.toJsonString(structuredRecord)));
      } else {
        if (structuredRecord.get(realtimeESSinkConfig.idField) == null) {
          LOG.debug("Found null data in id field. Skipping record.");
          continue;
        }
        bulkRequest.add(client.prepareIndex(realtimeESSinkConfig.index, realtimeESSinkConfig.type,
                            structuredRecord.get(realtimeESSinkConfig.idField).toString())
          .setSource(StructuredRecordStringConverter.toJsonString(structuredRecord)));
      }
      numRecordsWritten++;
    }

    BulkResponse response = bulkRequest.execute().actionGet();
    if (response.hasFailures()) {
      for (BulkItemResponse itemResponse : response.getItems()) {
        if (itemResponse.isFailed()) {
          numRecordsWritten--;
          LOG.debug(itemResponse.getFailureMessage());
        }
      }
    }
    return numRecordsWritten;
  }

  @Override
  public void destroy() {
    client.close();
  }

  /**
   * Config class for RealtimeElasticsearchSink.
   */
  public static class RealtimeESSinkConfig extends PluginConfig {

    @Name(Properties.Elasticsearch.INDEX_NAME)
    @Description(INDEX_DESCRIPTION)
    private String index;

    @Name(Properties.Elasticsearch.TYPE_NAME)
    @Description(TYPE_DESCRIPTION)
    private String type;

    @Name(Properties.Elasticsearch.ID_FIELD)
    @Description(ID_DESCRIPTION)
    @Nullable
    private String idField;

    @Name(Properties.Elasticsearch.TRANSPORT_ADDRESSES)
    @Description(TRANSPORT_ADDRESS_DESCRIPTION)
    private String transportAddresses;

    @Name(Properties.Elasticsearch.CLUSTER)
    @Description(CLUSTER_DESCRIPTION)
    @Nullable
    private String cluster;

    public RealtimeESSinkConfig(String index, String type, @Nullable String idField,
                                String transportAddresses, @Nullable String cluster) {
      this.index = index;
      this.type = type;
      this.idField = idField;
      this.transportAddresses = transportAddresses;
      this.cluster = cluster;
    }
  }
}
