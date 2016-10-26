package org.apache.hadoop.hive.ql.io;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.name.Names;
import com.metamx.common.Granularity;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.guice.GuiceInjectors;
import io.druid.initialization.Initialization;
import io.druid.query.DruidProcessingConfig;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.column.ColumnConfig;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.AppenderatorFactory;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
import io.druid.storage.hdfs.HdfsDataSegmentPusher;
import io.druid.storage.hdfs.HdfsDataSegmentPusherConfig;
import io.druid.timeline.DataSegment;
import io.druid.timeline.partition.LinearShardSpec;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordWriter;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.Progressable;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class DruidOutputFormat<K, V> implements HiveOutputFormat<K, DruidWritable>
{
  private static final String DRUID_MAX_PARTITION_SIZE = "druid.maxPartitionSize";
  private static final Integer DEFAULT_MAX_ROW_IN_MEMORY = 75000;
  private final HiveConf hiveConf = SessionState.get().getConf();

  public static class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
      org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter
  {
    protected static final Logger log = LoggerFactory.getLogger(DruidRecordWriter.class);
    private static final int DEFAULT_MAX_PARTITION_SIZE = 5000000;

    private final Injector injector;
    private final DataSchema dataSchema;
    private final Appenderator appenderator;
    private final RealtimeTuningConfig tuningConfig;
    private final Map<Long, List<SegmentIdentifier>> segments = new HashMap<>();
    private final Integer maxPartitionSize;

    public DruidRecordWriter(
        DataSchema dataSchema,
        RealtimeTuningConfig realtimeTuningConfig,
        Integer maxPartitionSize,
        final Path segmentLocation
    )
    {
      injector = Initialization.makeInjectorWithModules(
          GuiceInjectors.makeStartupInjector(),
          ImmutableList.<Module>of(new Module()
                                   {
                                     @Override
                                     public void configure(Binder binder)
                                     {
                                       binder.bindConstant().annotatedWith(Names.named("serviceName")).to("druid/indexer");
                                       binder.bindConstant().annotatedWith(Names.named("servicePort")).to(-1);
                                       HdfsDataSegmentPusherConfig hdfsDataSegmentPusherConfig = new HdfsDataSegmentPusherConfig();
                                       hdfsDataSegmentPusherConfig.setStorageDirectory(segmentLocation.toString());
                                       binder.bind(HdfsDataSegmentPusherConfig.class).toInstance(hdfsDataSegmentPusherConfig);
                                       binder.bind(DataSegmentPusher.class).to(HdfsDataSegmentPusher.class);
                                       binder.bind(DruidProcessingConfig.class).toInstance(
                                           new DruidProcessingConfig()
                                           {
                                             @Override
                                             public String getFormatString()
                                             {
                                               return "processing-%s";
                                             }

                                             @Override
                                             public int intermediateComputeSizeBytes()
                                             {
                                               return 100 * 1024 * 1024;
                                             }

                                             @Override
                                             public int getNumThreads()
                                             {
                                               return 1;
                                             }

                                             @Override
                                             public int columnCacheSizeBytes()
                                             {
                                               return 25 * 1024 * 1024;
                                             }
                                           }
                                       );
                                       binder.bind(ColumnConfig.class).to(DruidProcessingConfig.class);
                                     }
                                   }
          )
      );
      ObjectMapper objectMapper = injector.getInstance(ObjectMapper.class);
      AppenderatorFactory defaultOfflineAppenderatorFactory = null;
      try {
        defaultOfflineAppenderatorFactory = objectMapper.readerFor(AppenderatorFactory.class)
                                                        .readValue("{\"type\":\"offline\"}");
      }
      catch (IOException e) {
        Throwables.propagate(e);
      }
      this.tuningConfig = Preconditions.checkNotNull(realtimeTuningConfig);


      this.dataSchema = dataSchema;
      appenderator = defaultOfflineAppenderatorFactory.build(
          this.dataSchema,
          tuningConfig,
          new FireDepartmentMetrics()
      );
      this.maxPartitionSize = maxPartitionSize == null ? DEFAULT_MAX_PARTITION_SIZE : maxPartitionSize;

      appenderator.startJob(); // maybe we need to move this out of the constructor
    }

    private SegmentIdentifier getSegmentIdentifier(DruidWritable druidHiveRecord)
    {
      return getSegmentIdentifier(Longs.tryParse((String) druidHiveRecord.getValue()
                                                                         .get(DruidTable.DEFAULT_TIMESTAMP_COLUMN)));
    }

    private SegmentIdentifier getSegmentIdentifier(long timestamp)
    {

      final Granularity segmentGranularity = dataSchema.getGranularitySpec().getSegmentGranularity();


      final long truncatedTime = segmentGranularity.truncate(new DateTime(timestamp)).getMillis();

      List<SegmentIdentifier> segmentIdentifierList = segments.get(truncatedTime);

      final Interval interval = new Interval(
          new DateTime(truncatedTime),
          segmentGranularity.increment(new DateTime(truncatedTime))
      );

      SegmentIdentifier retVal;
      if (segmentIdentifierList == null) {

        retVal = new SegmentIdentifier(
            dataSchema.getDataSource(),
            interval,
            tuningConfig.getVersioningPolicy().getVersion(interval),
            new LinearShardSpec(0)
        );
        segments.put(truncatedTime, Arrays.asList(retVal));
        return retVal;
      } else {
        retVal = segmentIdentifierList.get(segmentIdentifierList.size());
        int rowCount = appenderator.getRowCount(retVal);
        if (rowCount < maxPartitionSize) {
          return retVal;
        } else {
          retVal = new SegmentIdentifier(
              dataSchema.getDataSource(),
              interval,
              tuningConfig.getVersioningPolicy().getVersion(interval),
              new LinearShardSpec(segmentIdentifierList.size())
          );
          segmentIdentifierList.add(retVal);
          return retVal;
        }
      }
    }

    @Override
    public void write(Writable w) throws IOException
    {
      DruidWritable record = (DruidWritable) w;
      InputRow inputRow = new MapBasedInputRow(
          Longs.tryParse((String) record.getValue()
                                        .get(DruidTable.DEFAULT_TIMESTAMP_COLUMN)),
          dataSchema.getParser()
                    .getParseSpec()
                    .getDimensionsSpec()
                    .getDimensionNames(),
          record.getValue()
      );

      try {
        appenderator.add(getSegmentIdentifier(record), inputRow, Suppliers.ofInstance(Committers.nil()));
      }
      catch (SegmentNotWritableException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(boolean abort) throws IOException
    {
      // Assuming we don't need to push the segment if we have to abort
      if (abort == true) {
        try {
          appenderator.clear(); // this is blocking
          return;
        }
        catch (InterruptedException e) {
          Thread.currentThread().interrupt();
        }
        finally {
          appenderator.close();
        }
      }

      final List<SegmentIdentifier> segmentsToPush = Lists.newArrayList();
      segmentsToPush.addAll(appenderator.getSegments());

      Futures.addCallback(
          appenderator.push(segmentsToPush, Committers.nil()),
          new FutureCallback<SegmentsAndMetadata>()
          {
            @Override
            public void onSuccess(SegmentsAndMetadata result)
            {
              // maybe immediately publish after pushing or do it later
              for (DataSegment pushedSegment : result.getSegments()) {
                try {
                  log.info("did pushed the segment [%s]", pushedSegment);
                  // maybe write to disk the segment location to publish ?
                }
                catch (Exception e) {
                  Throwables.propagate(e);
                }
              }

              log.info("Published [%,d] sinks.", segmentsToPush.size());
            }

            @Override
            public void onFailure(Throwable e)
            {
              log.warn(String.format("Failed to push [%,d] segments.", segmentsToPush.size()), e);
            }
          }
      );
      appenderator.close();
    }


    @Override
    public void write(NullWritable key, DruidWritable value) throws IOException
    {
      InputRow inputRow = new MapBasedInputRow(
          Longs.tryParse((String) value.getValue()
                                       .get(DruidTable.DEFAULT_TIMESTAMP_COLUMN)),
          dataSchema.getParser()
                    .getParseSpec()
                    .getDimensionsSpec()
                    .getDimensionNames(),
          value.getValue()
      );

      try {

        appenderator.add(getSegmentIdentifier(value), inputRow, null);
      }
      catch (SegmentNotWritableException e) {
        throw new IOException(e);
      }
    }

    @Override
    public void close(Reporter reporter) throws IOException
    {
      appenderator.close();
    }
  }

  @Override
  public FileSinkOperator.RecordWriter getHiveRecordWriter(
      JobConf jc,
      Path finalOutPath,
      Class<? extends Writable> valueClass,
      boolean isCompressed,
      Properties tableProperties,
      Progressable progress
  ) throws IOException
  {
    final String segmentGranularity = tableProperties.getProperty(Constants.DRUID_SEGMENT_GRANULARITY);
    final String dataSource = tableProperties.getProperty(Constants.DRUID_DATA_SOURCE);
    final InputRowParser inputRowParser = new MapInputRowParser(new TimeAndDimsParseSpec(
        new TimestampSpec(DruidTable.DEFAULT_TIMESTAMP_COLUMN, "auto", null),
        new DimensionsSpec(null, null, null)
    ));
    final AggregatorFactory[] aggregatorFactories = DruidStorageHandlerUtils.JSON_MAPPER.readValue(
        tableProperties.getProperty(Constants.DRUID_AGGREGATORS),
        AggregatorFactory[].class
    );
    final GranularitySpec granularitySpec = new UniformGranularitySpec(
        Granularity.valueOf(segmentGranularity),
        null,
        null
    );
    Map<String, Object> inputParser = DruidStorageHandlerUtils.JSON_MAPPER.convertValue(inputRowParser, Map.class);
    final DataSchema dataSchema = new DataSchema(
        dataSource,
        inputParser,
        aggregatorFactories,
        granularitySpec,
        DruidStorageHandlerUtils.JSON_MAPPER
    );

    // this can be initialized from the hive conf
    RealtimeTuningConfig realtimeTuningConfig = new RealtimeTuningConfig(
        DEFAULT_MAX_ROW_IN_MEMORY,
        null,
        null,
        new File("/tmp"),
        new CustomVersioningPolicy(null),
        null,
        null,
        null,
        null,
        null,
        0,
        0,
        null,
        null
    );

    String maxPartitionSizeString = tableProperties.getProperty(DRUID_MAX_PARTITION_SIZE, null);
    Integer maxPartitionSize = maxPartitionSizeString == null ? null : Integer.parseInt(maxPartitionSizeString);
    return new DruidRecordWriter(dataSchema, realtimeTuningConfig, maxPartitionSize, finalOutPath);
  }

  @Override
  public RecordWriter<K, DruidWritable> getRecordWriter(
      FileSystem ignored, JobConf job, String name, Progressable progress
  ) throws IOException
  {
    throw new UnsupportedOperationException("please implement me !");
  }

  @Override
  public void checkOutputSpecs(FileSystem ignored, JobConf job) throws IOException
  {
    throw new UnsupportedOperationException("not implemented yet");
  }
}
