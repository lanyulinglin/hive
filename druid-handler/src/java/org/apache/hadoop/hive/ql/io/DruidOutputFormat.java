package org.apache.hadoop.hive.ql.io;


import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.google.common.primitives.Longs;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.metamx.common.Granularity;
import io.druid.data.input.InputRow;
import io.druid.data.input.MapBasedInputRow;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.InputRowParser;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimeAndDimsParseSpec;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.GranularitySpec;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import io.druid.segment.loading.DataSegmentPusher;
import io.druid.segment.realtime.FireDepartmentMetrics;
import io.druid.segment.realtime.appenderator.Appenderator;
import io.druid.segment.realtime.appenderator.DefaultOfflineAppenderatorFactory;
import io.druid.segment.realtime.appenderator.SegmentIdentifier;
import io.druid.segment.realtime.appenderator.SegmentNotWritableException;
import io.druid.segment.realtime.appenderator.SegmentsAndMetadata;
import io.druid.segment.realtime.plumber.Committers;
import io.druid.segment.realtime.plumber.CustomVersioningPolicy;
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
  private static final int DEFAULT_MAX_PARTITION_SIZE = 5000000;

  public static class DruidRecordWriter implements RecordWriter<NullWritable, DruidWritable>,
      org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter
  {
    protected static final Logger log = LoggerFactory.getLogger(DruidRecordWriter.class);

    private final DataSchema dataSchema;
    private final Appenderator appenderator;
    private final RealtimeTuningConfig tuningConfig;
    private final Map<Long, List<SegmentIdentifier>> segments = new HashMap<>();
    private final Integer maxPartitionSize;
    private final Path segmentDescriptorDir;
    private final FileSystem fileSystem;

    public DruidRecordWriter(
        DataSchema dataSchema,
        RealtimeTuningConfig realtimeTuningConfig,
        Integer maxPartitionSize,
        final Path segmentDescriptorDir,
        final FileSystem fileSystem
    )
    {

      DefaultOfflineAppenderatorFactory defaultOfflineAppenderatorFactory = new DefaultOfflineAppenderatorFactory(
          DruidOutputFormatUtils.injector.getInstance(DataSegmentPusher.class),
          DruidStorageHandlerUtils.JSON_MAPPER,
          DruidOutputFormatUtils.INDEX_IO,
          DruidOutputFormatUtils.INDEX_MERGER
      );
      this.tuningConfig = realtimeTuningConfig;
      this.dataSchema = dataSchema;
      appenderator = defaultOfflineAppenderatorFactory.build(
          this.dataSchema,
          tuningConfig,
          new FireDepartmentMetrics()
      );
      this.maxPartitionSize = maxPartitionSize == null ? DEFAULT_MAX_PARTITION_SIZE : maxPartitionSize;

      appenderator.startJob(); // maybe we need to move this out of the constructor
      this.segmentDescriptorDir = Preconditions.checkNotNull(segmentDescriptorDir.getParent());
      this.fileSystem = Preconditions.checkNotNull(fileSystem);
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
      if (w == null) {
        return;
      }
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
      // @TODO ask about this abort. Assuming we don't need to push the segment if we have to abort
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
              for (DataSegment pushedSegment : result.getSegments()) {
                try {
                  final Path segmentOutputPath = makeOutputPath(pushedSegment);
                  DruidOutputFormatUtils.writeSegmentDescriptor(fileSystem, pushedSegment, segmentOutputPath);
                  log.info("did pushed the segment [%s] and persisted the descriptor located at [%s]", pushedSegment, segmentOutputPath);
                }
                catch (Exception e) {
                  Throwables.propagate(e);
                }
              }
              log.info("Published [%,d] segments.", segmentsToPush.size());
            }


            @Override
            public void onFailure(Throwable e)
            {
              log.warn(String.format("Failed to push [%,d] segments.", segmentsToPush.size()), e);
              Throwables.propagate(e);
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

    private Path makeOutputPath(DataSegment pushedSegment) {
      return new Path(segmentDescriptorDir, String.format("%s.json", pushedSegment.getIdentifier().replace(":", "")));
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

    Integer maxPartitionSize = hiveConf.getInt(DRUID_MAX_PARTITION_SIZE, DEFAULT_MAX_PARTITION_SIZE);
    return new DruidRecordWriter(dataSchema, realtimeTuningConfig, maxPartitionSize, finalOutPath, finalOutPath.getFileSystem(jc));
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
