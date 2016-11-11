/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.druid.io;

import com.google.common.base.Preconditions;
import io.druid.java.util.common.Granularity;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidSerDeUtils;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.util.Progressable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

import static org.apache.hadoop.hive.druid.DruidStorageHandler.SEGMENTS_DESCRIPTOR_DIR_NAME;

public class DruidOutputFormat<K, V> implements HiveOutputFormat<K, DruidWritable> {

  protected static final Logger LOG = LoggerFactory.getLogger(DruidOutputFormat.class);

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
          JobConf jc,
          Path finalOutPath,
          Class<? extends Writable> valueClass,
          boolean isCompressed,
          Properties tableProperties,
          Progressable progress
  ) throws IOException {

    final String segmentGranularity =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) != null ?
                    tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY) :
                    HiveConf.getVar(jc, HiveConf.ConfVars.HIVE_DRUID_INDEXING_GRANULARITY);
    final String dataSource = tableProperties.getProperty(Constants.DRUID_DATA_SOURCE);
    final String segmentDirectory =
            tableProperties.getProperty(Constants.DRUID_SEGMENT_DIRECTORY) != null
                    ? tableProperties.getProperty(Constants.DRUID_SEGMENT_DIRECTORY)
                    : HiveConf.getVar(jc, HiveConf.ConfVars.DRUID_SEGMENT_DIRECTORY);

    final HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
    hdfsDataSegmentPusherConfig.setStorageDirectory(segmentDirectory);
    final DataSegmentPusher hdfsDataSegmentPusher = new HdfsDataSegmentPusher(
            hdfsDataSegmentPusherConfig, jc, DruidStorageHandlerUtils.JSON_MAPPER);

    final GranularitySpec granularitySpec = new UniformGranularitySpec(
            Granularity.valueOf(segmentGranularity),
            null,
            null
    );
    final DataSchema dataSchema = new DataSchema(
            Preconditions.checkNotNull(dataSource, "Data Source is null"),
            DruidStorageHandlerUtils.getDefaultInputRowParser(),
            DruidSerDeUtils.fromTableProperties(tableProperties),
            granularitySpec,
            DruidStorageHandlerUtils.JSON_MAPPER
    );

    Integer maxPartitionSize = HiveConf
            .getIntVar(jc, HiveConf.ConfVars.HIVE_DRUID_MAX_PARTITION_SIZE);
    String basePersistDirectory = HiveConf
            .getVar(jc, HiveConf.ConfVars.HIVE_DRUID_BASE_PERSIST_DIRECTORY);
    final RealtimeTuningConfig realtimeTuningConfig = RealtimeTuningConfig
            .makeDefaultTuningConfig(new File(
                    basePersistDirectory)).withVersioningPolicy(new CustomVersioningPolicy(null));
    LOG.debug(String.format("running with DataSchema [%s] ", dataSchema));
    return new DruidRecordWriter(
            dataSchema,
            realtimeTuningConfig,
            hdfsDataSegmentPusher, maxPartitionSize,
            makeSegmentDescriptorOutputDir(finalOutPath),
            finalOutPath.getFileSystem(jc)
    );
  }

  private Path makeSegmentDescriptorOutputDir(Path finalOutPath) {
    return new Path(finalOutPath, SEGMENTS_DESCRIPTOR_DIR_NAME);
  }

  @Override
  public RecordWriter<K, DruidWritable> getRecordWriter(
          FileSystem ignored, JobConf job, String name, Progressable progress
  ) throws IOException {
    throw new UnsupportedOperationException("please implement me !");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
