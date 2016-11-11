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

import com.google.common.collect.ImmutableList;
import io.druid.data.input.impl.DimensionSchema;
import io.druid.data.input.impl.StringDimensionSchema;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.DoubleSumAggregatorFactory;
import io.druid.query.aggregation.LongSumAggregatorFactory;
import org.apache.calcite.adapter.druid.DruidTable;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Utils class for Druid SerDe.
 */
public final class DruidSerDeUtils {

  private static final Logger LOG = LoggerFactory.getLogger(DruidSerDeUtils.class);

  protected static final String FLOAT_TYPE = "FLOAT";

  protected static final String LONG_TYPE = "LONG";

  protected static final String STRING_TYPE = "STRING";

  /* This method converts from the String representation of Druid type
   * to the corresponding Hive type */
  public static PrimitiveTypeInfo convertDruidToHiveType(String typeName) {
    typeName = typeName.toUpperCase();
    switch (typeName) {
      case FLOAT_TYPE:
        return TypeInfoFactory.floatTypeInfo;
      case LONG_TYPE:
        return TypeInfoFactory.longTypeInfo;
      case STRING_TYPE:
        return TypeInfoFactory.stringTypeInfo;
      default:
        // This is a guard for special Druid types e.g. hyperUnique
        // (http://druid.io/docs/0.9.1.1/querying/aggregations.html#hyperunique-aggregator).
        // Currently, we do not support doing anything special with them in Hive.
        // However, those columns are there, and they can be actually read as normal
        // dimensions e.g. with a select query. Thus, we print the warning and just read them
        // as String.
        LOG.warn("Transformation to STRING for unknown type " + typeName);
        return TypeInfoFactory.stringTypeInfo;
    }
  }

  /* This method converts from the String representation of Druid type
   * to the String representation of the corresponding Hive type */
  public static String convertDruidToHiveTypeString(String typeName) {
    typeName = typeName.toUpperCase();
    switch (typeName) {
      case FLOAT_TYPE:
        return serdeConstants.FLOAT_TYPE_NAME;
      case LONG_TYPE:
        return serdeConstants.BIGINT_TYPE_NAME;
      case STRING_TYPE:
        return serdeConstants.STRING_TYPE_NAME;
      default:
        // This is a guard for special Druid types e.g. hyperUnique
        // (http://druid.io/docs/0.9.1.1/querying/aggregations.html#hyperunique-aggregator).
        // Currently, we do not support doing anything special with them in Hive.
        // However, those columns are there, and they can be actually read as normal
        // dimensions e.g. with a select query. Thus, we print the warning and just read them
        // as String.
        LOG.warn("Transformation to STRING for unknown type " + typeName);
        return serdeConstants.STRING_TYPE_NAME;
    }
  }

  public static AggregatorFactory[] fromTableProperties(final Properties tableProperties) {
    // Parse the configuration parameters
    final String columnNameProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tableProperties.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (StringUtils.isEmpty(columnNameProperty) || StringUtils.isEmpty(columnTypeProperty)) {
      throw new IllegalStateException(
              String.format("List of columns names [%s] or columns type [%s] is/are not present",
                      columnNameProperty, columnTypeProperty
              ));
    }
    ArrayList<String> columnNames = new ArrayList<String>();
    for (String name : columnNameProperty.split(",")) {
      columnNames.add(name);
    }
    if (!columnNames.contains(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
      throw new IllegalStateException("Timestamp column (' " + DruidTable.DEFAULT_TIMESTAMP_COLUMN +
              "') not specified in create table; list of columns is : " +
              tableProperties.getProperty(serdeConstants.LIST_COLUMNS));
    }
    ArrayList<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    // Default, all columns that are not metrics or timestamp, are treated as dimensions
    final List<DimensionSchema> dimensions = new ArrayList<>();
    ImmutableList.Builder<AggregatorFactory> aggregatorFactoryBuilder = ImmutableList.builder();
    for (int i = 0; i < columnTypes.size(); i++) {
      TypeInfo f = columnTypes.get(i);
      assert f.getCategory() == ObjectInspector.Category.PRIMITIVE;
      AggregatorFactory af;
      switch (f.getTypeName()) {
        case serdeConstants.TINYINT_TYPE_NAME:
        case serdeConstants.SMALLINT_TYPE_NAME:
        case serdeConstants.INT_TYPE_NAME:
        case serdeConstants.BIGINT_TYPE_NAME:
          af = new LongSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
          break;
        case serdeConstants.FLOAT_TYPE_NAME:
        case serdeConstants.DOUBLE_TYPE_NAME:
          af = new DoubleSumAggregatorFactory(columnNames.get(i), columnNames.get(i));
          break;
        default:
          // Dimension or timestamp
          String columnName = columnNames.get(i);
          if (!columnName.equals(DruidTable.DEFAULT_TIMESTAMP_COLUMN)) {
            dimensions.add(new StringDimensionSchema(columnName));
          }
          continue;
      }
      aggregatorFactoryBuilder.add(af);
    }
    List<AggregatorFactory> aggregatorFactories = aggregatorFactoryBuilder.build();

    return aggregatorFactories.toArray(new AggregatorFactory[aggregatorFactories.size()]);
  }

}
