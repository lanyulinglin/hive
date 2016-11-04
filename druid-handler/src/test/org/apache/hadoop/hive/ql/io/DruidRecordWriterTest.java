package org.apache.hadoop.hive.ql.io;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.druid.data.input.impl.DimensionsSpec;
import io.druid.data.input.impl.JSONParseSpec;
import io.druid.data.input.impl.MapInputRowParser;
import io.druid.data.input.impl.TimestampSpec;
import io.druid.granularity.QueryGranularities;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.java.util.common.Granularity;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.CountAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import io.druid.segment.indexing.DataSchema;
import io.druid.segment.indexing.RealtimeTuningConfig;
import io.druid.segment.indexing.granularity.UniformGranularitySpec;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.serde.DruidWritable;
import org.apache.hadoop.mapred.JobConf;
import org.joda.time.DateTime;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Map;

public class DruidRecordWriterTest
{
  private ObjectMapper objectMapper = new DefaultObjectMapper();

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private DruidOutputFormat.DruidRecordWriter druidRecordWriter;
  @Test
  public void testWrite() throws IOException
  {
    final Map<String, Object> parserMap = objectMapper.convertValue(
        new MapInputRowParser(
            new JSONParseSpec(
                new TimestampSpec("__time", "auto", null),
                new DimensionsSpec(null, null, null),
                null,
                null
            )
        ),
        Map.class
    );
    DataSchema dataSchema = new DataSchema(
        "dataSourceName",
        parserMap,
        new AggregatorFactory[]{
            new CountAggregatorFactory("count"),
            new LongSumAggregatorFactory("met", "met")
        },
        new UniformGranularitySpec(Granularity.MINUTE, QueryGranularities.NONE, null),
        objectMapper
    );

    RealtimeTuningConfig tuningConfig = new RealtimeTuningConfig(
        75000,
        null,
        null,
        temporaryFolder.newFolder(),
        null,
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



    Configuration config = new Configuration();
    JobConf jobConf = new JobConf(config);
    LocalFileSystem localFileSystem = FileSystem.getLocal(config);

    druidRecordWriter = new DruidOutputFormat.DruidRecordWriter(dataSchema, tuningConfig, 20, new Path("/tmp/slim/test"), localFileSystem, jobConf);
    druidRecordWriter.write(null);
    DruidWritable druidWritable = new DruidWritable(ImmutableMap.<String, Object>of(
        DruidTable.DEFAULT_TIMESTAMP_COLUMN,
        String.valueOf(new DateTime().getMillis()),
        Constants.DRUID_TIMESTAMP_GRANULARITY_COL_NAME,
        String.valueOf(Granularity.MINUTE.truncate(new DateTime()).getMillis()),
        "dim",
        "test",
        "met",
        "1"
    ));
    druidRecordWriter.write(druidWritable);
    druidRecordWriter.close(false);

  }

}