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
package org.apache.hadoop.hive.druid;

import com.google.common.base.Preconditions;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.Interner;
import com.google.common.collect.Interners;
import com.google.common.collect.Lists;
import io.druid.indexer.SQLMetadataStorageUpdaterJobHandler;
import io.druid.java.util.common.MapUtils;
import io.druid.metadata.MetadataStorageConnectorConfig;
import io.druid.metadata.MetadataStorageTablesConfig;
import io.druid.metadata.SQLMetadataConnector;
import io.druid.metadata.storage.mysql.MySQLConnector;
import io.druid.segment.loading.SegmentLoadingException;
import io.druid.timeline.DataSegment;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.Constants;
import org.apache.hadoop.hive.druid.io.DruidOutputFormat;
import org.apache.hadoop.hive.druid.io.DruidQueryBasedInputFormat;
import org.apache.hadoop.hive.druid.serde.DruidSerDe;
import org.apache.hadoop.hive.metastore.HiveMetaHook;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.metadata.DefaultStorageHandler;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.OutputFormat;
import org.skife.jdbi.v2.FoldController;
import org.skife.jdbi.v2.Folder3;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.util.ByteArrayMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * DruidStorageHandler provides a HiveStorageHandler implementation for Druid.
 */
@SuppressWarnings({"deprecation", "rawtypes"})
public class DruidStorageHandler extends DefaultStorageHandler implements HiveMetaHook
{

  protected static final Logger LOG = LoggerFactory.getLogger(DruidStorageHandler.class);
  private static final Interner<DataSegment> DATA_SEGMENT_INTERNER = Interners.newWeakInterner();

  public static final String SEGMENTS_DESCRIPTOR_DIR_NAME = "segmentsDescriptorDir";

  private final SQLMetadataConnector connector;
  private final SQLMetadataStorageUpdaterJobHandler druidSqlMetadataStorageUpdaterJobHandler;
  private final MetadataStorageTablesConfig druidMetadataStorageTablesConfig = MetadataStorageTablesConfig.fromBase("druid");

  public DruidStorageHandler()
  {
    final String dbType = SessionState.getSessionConf().get("hive.druid.metadata.db.type", "mysql");
    final String username = SessionState.getSessionConf().get("hive.druid.metadata.username", "druid");
    final String password = SessionState.getSessionConf().get("hive.druid.metadata.password", "");
    final String uri = SessionState.getSessionConf().get("hive.druid.metadata.uri", "jdbc:mysql://cn105-10.l42scl.hortonworks.com/druid_db");

    connector = new MySQLConnector(Suppliers.<MetadataStorageConnectorConfig>ofInstance(new MetadataStorageConnectorConfig()
    {
      @Override
      public String getConnectURI()
      {
        return uri;
      }

      @Override
      public String getUser()
      {
        return username;
      }

      @Override
      public String getPassword()
      {
        return password;
      }
    }), Suppliers.ofInstance(druidMetadataStorageTablesConfig));

    druidSqlMetadataStorageUpdaterJobHandler = new SQLMetadataStorageUpdaterJobHandler(connector);
  }

  @Override
  public Class<? extends InputFormat> getInputFormatClass()
  {
    return DruidQueryBasedInputFormat.class;
  }

  @Override
  public Class<? extends OutputFormat> getOutputFormatClass()
  {
    return DruidOutputFormat.class;
  }

  @Override
  public Class<? extends SerDe> getSerDeClass()
  {
    return DruidSerDe.class;
  }

  @Override
  public HiveMetaHook getMetaHook()
  {
    return this;
  }

  @Override
  public void preCreateTable(Table table) throws MetaException
  {
    // Do safety checks
    if (MetaStoreUtils.isExternalTable(table) && !StringUtils.isEmpty(table.getSd().getLocation())) {
      throw new MetaException("LOCATION may not be specified for Druid existing sources");
    }
    if (table.getPartitionKeysSize() != 0) {
      throw new MetaException("PARTITIONED BY may not be specified for Druid");
    }
    if (table.getSd().getBucketColsSize() != 0) {
      throw new MetaException("CLUSTERED BY may not be specified for Druid");
    }
    String dataSourceName = Preconditions.checkNotNull(table.getParameters().get(Constants.DRUID_DATA_SOURCE), "WTF dataSource name is null !");

    Collection<String> existingDataSources = getAllDatasourceNames();
    LOG.debug(String.format("pre create datasource [%s]", dataSourceName));
    LOG.info(String.format("Existing data sources [%s]", existingDataSources));
    if (existingDataSources.contains(dataSourceName)) {
      throw new IllegalStateException(String.format("Data source [%s] already existing please drop the data source or use insert statement", dataSourceName));
    }
  }

  @Override
  public void rollbackCreateTable(Table table) throws MetaException
  {
    final Path segmentDescriptorDir = new Path(table.getSd().getLocation());
    try {
      List<DataSegment> dataSegmentList = DruidStorageHandlerUtils
          .getPublishedSegments(segmentDescriptorDir, getConf());
      for (DataSegment dataSegment :
          dataSegmentList) {
        try {
          deleteSegment(dataSegment);
        }
        catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while trying to clean the segment [%s]", dataSegment), e);
        }
      }
    }
    catch (IOException e) {
      LOG.error("Exception while rollback", e);
      Throwables.propagate(e);
    }
  }

  @Override
  public void commitCreateTable(Table table) throws MetaException
  {
    final Path segmentDescriptorDir = new Path(table.getSd().getLocation());
    try {
      publishSegments(DruidStorageHandlerUtils.getPublishedSegments(segmentDescriptorDir, getConf()));
    }
    catch (IOException e) {
      LOG.error("Exception while commit", e);
      Throwables.propagate(e);
    }
  }

  public void deleteSegment(DataSegment segment) throws SegmentLoadingException
  {

    final Path path = getPath(segment);
    LOG.info(String.format("removing segment[%s] mapped to path[%s]", segment.getIdentifier(), path));

    try {
      if (path.getName().endsWith(".zip")) {

        final FileSystem fs = path.getFileSystem(getConf());

        if (!fs.exists(path)) {
          LOG.warn(String.format("Segment Path [%s] does not exist. It appears to have been deleted already.", path));
          return;
        }

        // path format -- > .../dataSource/interval/version/partitionNum/xxx.zip
        Path partitionNumDir = path.getParent();
        if (!fs.delete(partitionNumDir, true)) {
          throw new SegmentLoadingException(
              "Unable to kill segment, failed to delete dir [%s]",
              partitionNumDir.toString()
          );
        }

        //try to delete other directories if possible
        Path versionDir = partitionNumDir.getParent();
        if (safeNonRecursiveDelete(fs, versionDir)) {
          Path intervalDir = versionDir.getParent();
          if (safeNonRecursiveDelete(fs, intervalDir)) {
            Path dataSourceDir = intervalDir.getParent();
            safeNonRecursiveDelete(fs, dataSourceDir);
          }
        }
      } else {
        throw new SegmentLoadingException("Unknown file type[%s]", path);
      }
    }
    catch (IOException e) {
      throw new SegmentLoadingException(e, "Unable to kill segment");
    }
  }

  private Path getPath(DataSegment dataSegment)
  {
    return new Path(String.valueOf(dataSegment.getLoadSpec().get("path")));
  }

  private boolean safeNonRecursiveDelete(FileSystem fs, Path path)
  {
    try {
      return fs.delete(path, false);
    }
    catch (Exception ex) {
      return false;
    }
  }

  private void publishSegments(List<DataSegment> publishedSegments)
  {

    druidSqlMetadataStorageUpdaterJobHandler.publishSegments(
        druidMetadataStorageTablesConfig.getSegmentsTable(),
        publishedSegments,
        DruidStorageHandlerUtils.JSON_MAPPER
    );
    return;
  }

  private Collection<String> getAllDatasourceNames()
  {
      return connector.getDBI().withHandle(
              new HandleCallback<List<String>>()
              {
                @Override
                public List<String> withHandle(Handle handle) throws Exception
                {
                  return handle.createQuery(
                          String.format("SELECT DISTINCT(datasource) FROM %s WHERE used = true", druidMetadataStorageTablesConfig.getSegmentsTable())
                  )
                          .fold(
                                  Lists.<String>newArrayList(),
                                  new Folder3<ArrayList<String>, Map<String, Object>>()
                                  {
                                    @Override
                                    public ArrayList<String> fold(
                                            ArrayList<String> druidDataSources,
                                            Map<String, Object> stringObjectMap,
                                            FoldController foldController,
                                            StatementContext statementContext
                                    ) throws SQLException
                                    {
                                      druidDataSources.add(
                                              MapUtils.getString(stringObjectMap, "datasource")
                                      );
                                      return druidDataSources;
                                    }
                                  }
                          );

                }
              }
      );

  }

  private boolean removeDatasource(final String dataSource)
  {
    try {
      if (!getAllDatasourceNames().contains(dataSource)) {
        LOG.warn(String.format("Cannot delete datasource %s, does not exist", dataSource));
        return false;
      }

      connector.getDBI().withHandle(
              new HandleCallback<Void>()
              {
                @Override
                public Void withHandle(Handle handle) throws Exception
                {
                  handle.createStatement(
                          String.format("UPDATE %s SET used=false WHERE dataSource = :dataSource", druidMetadataStorageTablesConfig.getSegmentsTable())
                  )
                          .bind("dataSource", dataSource)
                          .execute();

                  return null;
                }
              }
      );

    }
    catch (Exception e) {
      LOG.error(String.format("Error removing dataSource %s", dataSource), e);
      return false;
    }
    return true;
  }

  private List<DataSegment> getDataSegment(final String dataSource)
  {
    List<DataSegment> segmentList = connector.retryTransaction(
            new TransactionCallback<List<DataSegment>>()
            {
              @Override
              public List<DataSegment> inTransaction(
                      Handle handle, TransactionStatus status
              ) throws Exception
              {
                return handle
                        .createQuery(String.format(
                                "SELECT payload FROM %s WHERE dataSource = :dataSource",
                                druidMetadataStorageTablesConfig.getSegmentsTable()
                        ))
                        .setFetchSize(getStreamingFetchSize())
                        .bind("dataSource", dataSource)
                        .map(ByteArrayMapper.FIRST)
                        .fold(
                                new ArrayList<DataSegment>(),
                                new Folder3<List<DataSegment>, byte[]>()
                                {
                                  @Override
                                  public List<DataSegment> fold(List<DataSegment> accumulator,
                                          byte[] payload, FoldController control,
                                          StatementContext ctx
                                  ) throws SQLException
                                  {
                                    try {
                                      final DataSegment segment = DATA_SEGMENT_INTERNER.intern(DruidStorageHandlerUtils.JSON_MAPPER.readValue(
                                              payload,
                                              DataSegment.class
                                      ));

                                      accumulator.add(segment);
                                      return accumulator;
                                    }
                                    catch (Exception e) {
                                      throw new SQLException(e.toString());
                                    }
                                  }
                                }
                        );
              }
            }
    , 3, SQLMetadataConnector.DEFAULT_MAX_TRIES);
    return segmentList;
  }

  private int getStreamingFetchSize() {
    //@TODO this works only for my-sql case
    return Integer.MIN_VALUE;
  }

  @Override
  public void preDropTable(Table table) throws MetaException
  {
    // Nothing to do
  }

  @Override
  public void rollbackDropTable(Table table) throws MetaException
  {
    // Nothing to do
  }

  @Override
  public void commitDropTable(Table table, boolean deleteData) throws MetaException
  {
    String dataSourceName = Preconditions.checkNotNull(table.getParameters().get(Constants.DRUID_DATA_SOURCE), "WTF dataSource name is null !");

    if (deleteData)
    {
      List<DataSegment> dataSegmentList = getDataSegment(dataSourceName);
      if (dataSegmentList.isEmpty()) {
        LOG.info(String.format("nothing to delete for data source [%s]", dataSourceName));
        return;
      }
      for (DataSegment dataSegment:
           dataSegmentList) {
        try {
          LOG.info(String.format("Deleting segment [%s] with path [%s]", dataSegment.getIdentifier(), dataSegment.getLoadSpec().get("path")));
          deleteSegment(dataSegment);
        } catch (SegmentLoadingException e) {
          LOG.error(String.format("Error while deleting segment [%s]", dataSegment.getIdentifier()), e);
        }
      }
    }
    if (removeDatasource(dataSourceName)) {
     LOG.info(String.format("Successfully dropped dataSource [%s]", dataSourceName));
    }
  }

  @Override
  public String toString()
  {
    return Constants.DRUID_HIVE_STORAGE_HANDLER_ID;
  }

}
