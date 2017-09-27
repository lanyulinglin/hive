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
package org.apache.hadoop.hive.druid.serde;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.metamx.http.client.HttpClient;
import io.druid.data.input.MapBasedRow;
import io.druid.query.dimension.DimensionSpec;
import io.druid.query.dimension.ExtractionDimensionSpec;
import io.druid.query.extraction.TimeFormatExtractionFn;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;

import com.fasterxml.jackson.core.type.TypeReference;

import io.druid.data.input.Row;
import io.druid.query.groupby.GroupByQuery;
import org.joda.time.format.ISODateTimeFormat;

import static org.apache.hadoop.hive.druid.serde.DruidSerDeUtils.ISO_TIME_FORMAT;

/**
 * Record reader for results for Druid GroupByQuery.
 */
public class DruidGroupByQueryRecordReader
        extends DruidQueryRecordReader<GroupByQuery, Row> {
  private final static TypeReference<Row> TYPE_REFERENCE = new TypeReference<Row>() {
  };

  private MapBasedRow currentRow;

  private List<String> timeExtractionFields = Lists.newArrayList();
  private List<String> intFormattedTimeExtractionFields = Lists.newArrayList();
/*
  // Grouping dimensions can have different types if we are grouping using an
  // extraction function
  private List<PrimitiveTypeInfo> dimensionTypes;

  // Row objects returned by GroupByQuery have different access paths depending on
  // whether the result for the metric is a Float or a Long, thus we keep track
  // using these converters
  private ImmutableList<Extract> extractors;
*/

  @Override
  public void initialize(InputSplit split, Configuration conf) throws IOException {
    super.initialize(split, conf);
    initDimensionTypes();
    /*initExtractors();*/
  }

  @Override
  public void initialize(InputSplit split, Configuration conf, ObjectMapper mapper,
          ObjectMapper smileMapper, HttpClient httpClient
  ) throws IOException {
    super.initialize(split, conf, mapper, smileMapper, httpClient);
    initDimensionTypes();
    /*initExtractors();*/
  }

  @Override
  protected JavaType getResultTypeDef() {
    return DruidStorageHandlerUtils.JSON_MAPPER.getTypeFactory().constructType(TYPE_REFERENCE);
  }

  private void initDimensionTypes() throws IOException {
    List<DimensionSpec> dimensionSpecList = ((GroupByQuery) query).getDimensions();
    List<DimensionSpec> extractionDimensionSpecList = dimensionSpecList.stream()
            .filter(dimensionSpecs -> dimensionSpecs instanceof ExtractionDimensionSpec)
            .collect(Collectors.toList());
    extractionDimensionSpecList.stream().forEach(dimensionSpec -> {
      ExtractionDimensionSpec extractionDimensionSpec = (ExtractionDimensionSpec) dimensionSpec;
      if (extractionDimensionSpec.getExtractionFn() instanceof TimeFormatExtractionFn) {
        final TimeFormatExtractionFn timeFormatExtractionFn = (TimeFormatExtractionFn) extractionDimensionSpec
                .getExtractionFn();
        if (timeFormatExtractionFn  == null || timeFormatExtractionFn.getFormat().equals(ISO_TIME_FORMAT)) {
          timeExtractionFields.add(extractionDimensionSpec.getOutputName());
        } else {
          intFormattedTimeExtractionFields.add(extractionDimensionSpec.getOutputName());
        }
      }
    });

  }

 /* private void initExtractors() throws IOException {

    List<Extract> aggregatorExtractors = Lists.transform(query.getAggregatorSpecs(),
            new Function<AggregatorFactory, Extract>() {
              @Nullable
              @Override
              public Extract apply(@Nullable AggregatorFactory aggregatorFactory
              ) {
                switch (aggregatorFactory.getTypeName().toUpperCase()) {
                case DruidSerDeUtils.FLOAT_TYPE:
                  return Extract.FLOAT;
                case DruidSerDeUtils.LONG_TYPE:
                  return Extract.LONG;
                case DruidSerDeUtils.DOUBLE_TYPE:
                  return Extract.DOUBLE;
                }
                throw new RuntimeException("Type [" + aggregatorFactory.getTypeName().toUpperCase()
                        + "] not supported");
              }
            }
    );*/

    /*List<Extract> postaggregateExtractor = Lists.transform(query.getPostAggregatorSpecs(),
            new Function<PostAggregator, Extract>() {
              @Nullable
              @Override
              public Extract apply(@Nullable PostAggregator postAggregator) {
                return Extract.FLOAT;
              }
            }
    );

    extractors = new ImmutableList.Builder<Extract>().addAll(aggregatorExtractors).addAll(postaggregateExtractor).build();

  }*/

  @Override
  public boolean nextKeyValue() {
    // Results

    if (queryResultsIterator.hasNext()) {
      final Row row = queryResultsIterator.next();
      if (row instanceof MapBasedRow) {
        currentRow = (MapBasedRow) row;
        return true;
      } else {
        throw new RuntimeException("don't know how to deal with Row type " + row.getClass());
      }
    }
    return false;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public DruidWritable getCurrentValue() throws IOException, InterruptedException {
    // Create new value
    DruidWritable value = new DruidWritable();
    final Map<String, Object> event = Maps.transformEntries(currentRow.getEvent(),
            (key, value1) -> {
              if (timeExtractionFields.contains(key)) {
                return ISODateTimeFormat.dateTimeParser().parseMillis((String) value1);
              }
              if (intFormattedTimeExtractionFields.contains(key)) {
                return Integer.valueOf((String) value1);
              }
              return value1;
            }
    );
    value.getValue().putAll(event);
    // 1) The timestamp column
    value.getValue().put(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN, currentRow.getTimestamp().getMillis());
    // 2) The dimension columns


    /*for (int i = 0; i < query.getDimensions().size(); i++) {
      DimensionSpec ds = query.getDimensions().get(i);
      List<String> dims = currentRow.getDimension(ds.getOutputName());
      if (dims.size() == 0) {
        // NULL value for dimension
        value.getValue().put(ds.getOutputName(), null);
      } else {
        int pos = dims.size() - indexes[i] - 1;
        Object val;
        switch (dimensionTypes.get(i).getPrimitiveCategory()) {
          case TIMESTAMP:
            // FLOOR extraction function
            val = ISODateTimeFormat.dateTimeParser().parseMillis((String) dims.get(pos));
            break;
          case INT:
            // EXTRACT extraction function
            val = Integer.valueOf((String) dims.get(pos));
            break;
          default:
            val = dims.get(pos);
        }
        value.getValue().put(ds.getOutputName(), val);
      }
    }
    int counter = 0;
    // 3) The aggregation columns
    for (AggregatorFactory af : query.getAggregatorSpecs()) {
      switch (extractors[counter++]) {
        case FLOAT:
          value.getValue().put(af.getName(), currentRow.getFloatMetric(af.getName()));
          break;
        case LONG:
          value.getValue().put(af.getName(), currentRow.getLongMetric(af.getName()));
          break;
      case DOUBLE:
          value.getValue().put(af.getName(), currentRow.getDoubleMetric(af.getName()));
          break;
      }
    }
    // 4) The post-aggregation columns
    for (PostAggregator pa : query.getPostAggregatorSpecs()) {
      assert extractors[counter++] == Extract.FLOAT;
      value.getValue().put(pa.getName(), currentRow.getFloatMetric(pa.getName()));
    }*/
    return value;
  }

  @Override
  public boolean next(NullWritable key, DruidWritable value) {
    if (nextKeyValue()) {
      // Update value
      value.getValue().clear();
      // 1) The timestamp column
      value.getValue().put(DruidStorageHandlerUtils.DEFAULT_TIMESTAMP_COLUMN, currentRow.getTimestamp().getMillis());
      // 2) The dimension columns
      value.getValue().putAll(currentRow.getEvent());

     /* for (int i = 0; i < query.getDimensions().size(); i++) {
        DimensionSpec ds = query.getDimensions().get(i);
        List<String> dims = currentRow.getDimension(ds.getOutputName());
        if (dims.size() == 0) {
          // NULL value for dimension
          value.getValue().put(ds.getOutputName(), null);
        } else {
          int pos = dims.size() - indexes[i] - 1;
          Object val;
          switch (dimensionTypes[i].getPrimitiveCategory()) {
            case TIMESTAMP:
              // FLOOR extraction function
              val = ISODateTimeFormat.dateTimeParser().parseMillis((String) dims.get(pos));
              break;
            case INT:
              // EXTRACT extraction function
              val = Integer.valueOf((String) dims.get(pos));
              break;
            default:
              val = dims.get(pos);
          }
          value.getValue().put(ds.getOutputName(), val);
        }
      }
      int counter = 0;
      // 3) The aggregation columns
      for (AggregatorFactory af : query.getAggregatorSpecs()) {
        switch (extractors[counter++]) {
        case FLOAT:
          value.getValue().put(af.getName(), currentRow.getFloatMetric(af.getName()));
          break;
        case LONG:
          value.getValue().put(af.getName(), currentRow.getLongMetric(af.getName()));
          break;
        case DOUBLE:
          value.getValue().put(af.getName(), currentRow.getDoubleMetric(af.getName()));
          break;
        }
      }
      // 4) The post-aggregation columns
      for (PostAggregator pa : query.getPostAggregatorSpecs()) {
        assert extractors[counter++] == Extract.FLOAT;
        value.getValue().put(pa.getName(), currentRow.getFloatMetric(pa.getName()));
      }*/
      return true;
    }
    return false;
  }

  @Override
  public float getProgress() throws IOException {
    return queryResultsIterator.hasNext() ? 0 : 1;
  }

}
