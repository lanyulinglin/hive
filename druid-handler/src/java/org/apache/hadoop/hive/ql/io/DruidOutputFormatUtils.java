package org.apache.hadoop.hive.ql.io;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Module;
import io.druid.guice.GuiceInjectors;
import io.druid.guice.JsonConfigProvider;
import io.druid.guice.annotations.Json;
import io.druid.guice.annotations.Self;
import io.druid.guice.annotations.Smile;
import io.druid.initialization.Initialization;
import io.druid.segment.IndexIO;
import io.druid.segment.IndexMerger;
import io.druid.segment.IndexMergerV9;
import io.druid.server.DruidNode;
import io.druid.storage.hdfs.HdfsStorageDruidModule;

public class DruidOutputFormatUtils
{
  public static final Injector injector;
  public static final ObjectMapper JSON_MAPPER;
  public static final IndexIO INDEX_IO;
  public static final IndexMerger INDEX_MERGER;
  public static final IndexMergerV9 INDEX_MERGER_V9;
  public static final ObjectMapper SMILE_MAPPER;

  static {
    injector = Initialization.makeInjectorWithModules(
        GuiceInjectors.makeStartupInjector(),
        ImmutableList.<Module>of(new Module()
        {
          @Override
          public void configure(Binder binder)
          {
            JsonConfigProvider.bindInstance(
                binder,
                Key.get(DruidNode.class, Self.class),
                new DruidNode("hive-druid", null, null)
            );
          }
        }, new HdfsStorageDruidModule())
    );
    JSON_MAPPER = injector.getInstance(Key.get(ObjectMapper.class, Json.class));
    SMILE_MAPPER = injector.getInstance(Key.get(ObjectMapper.class, Smile.class));
    INDEX_IO = injector.getInstance(IndexIO.class);
    INDEX_MERGER = injector.getInstance(IndexMerger.class);
    INDEX_MERGER_V9 = injector.getInstance(IndexMergerV9.class);
  }

}
