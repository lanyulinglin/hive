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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.ObjectCodec;
import com.fasterxml.jackson.databind.JavaType;
import com.google.common.base.Throwables;
import com.metamx.http.client.Request;
import com.metamx.http.client.response.InputStreamResponseHandler;
import io.druid.java.util.common.IAE;
import io.druid.java.util.common.RE;
import io.druid.java.util.common.guava.CloseQuietly;
import io.druid.query.BaseQuery;
import io.druid.query.Query;
import io.druid.query.QueryInterruptedException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.druid.DruidStorageHandler;
import org.apache.hadoop.hive.druid.DruidStorageHandlerUtils;
import org.apache.hadoop.hive.druid.io.HiveDruidSplit;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Base record reader for given a Druid query. This class contains the logic to
 * send the query to the broker and retrieve the results. The transformation to
 * emit records needs to be done by the classes that extend the reader.
 *
 * The key for each record will be a NullWritable, while the value will be a
 * DruidWritable containing the timestamp as well as all values resulting from
 * the query.
 */
public abstract class DruidQueryRecordReader<T extends BaseQuery<R>, R extends Comparable<R>>
        extends RecordReader<NullWritable, DruidWritable>
        implements org.apache.hadoop.mapred.RecordReader<NullWritable, DruidWritable> {

  private static final Logger LOG = LoggerFactory.getLogger(DruidQueryRecordReader.class);

  /**
   * Query that Druid executes.
   */
  protected T query;

  /**
   * Query results.
   */
  protected JsonParserIterator<R> queryResultsIterator =  null;
  protected JavaType resultsType = null;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
    initialize(split, context.getConfiguration());
  }

  public void initialize(InputSplit split, Configuration conf) throws IOException {
    HiveDruidSplit hiveDruidSplit = (HiveDruidSplit) split;

    // Create query
    query = createQuery(hiveDruidSplit.getDruidQuery());

    resultsType = getResultTypeDef();

    // Execute query
    if (LOG.isInfoEnabled()) {
      LOG.info("Retrieving from druid using query:\n " + query);
    }

    Request request = DruidStorageHandlerUtils
            .createRequest(hiveDruidSplit.getLocations()[0], query);

    Future<InputStream> inputStreamFuture = DruidStorageHandler.getHttpClient()
            .go(request, new InputStreamResponseHandler());

    queryResultsIterator = new JsonParserIterator(resultsType, inputStreamFuture,
            request.getUrl().toString(), query);
  }

  protected abstract T createQuery(String content) throws IOException;

  protected abstract JavaType getResultTypeDef();

  @Override
  public NullWritable createKey() {
    return NullWritable.get();
  }

  @Override
  public DruidWritable createValue() {
    return new DruidWritable();
  }

  @Override
  public abstract boolean next(NullWritable key, DruidWritable value) throws IOException;

  @Override
  public long getPos() {
    return 0;
  }

  @Override
  public abstract boolean nextKeyValue() throws IOException;

  @Override
  public abstract NullWritable getCurrentKey() throws IOException, InterruptedException;

  @Override
  // TODO: we could generate vector row batches so that vectorized execution may get triggered
  public abstract DruidWritable getCurrentValue() throws IOException, InterruptedException;

  @Override
  public abstract float getProgress() throws IOException;

  @Override
  public void close() {
    CloseQuietly.close(queryResultsIterator);
  }

  protected class JsonParserIterator<R extends Comparable<R>> implements Iterator<R>, Closeable
  {
    private JsonParser jp;
    private ObjectCodec objectCodec;
    private final JavaType typeRef;
    private final Future<InputStream> future;
    private final Query<T> query;
    private final String url;

    public JsonParserIterator(JavaType typeRef, Future<InputStream> future, String url, Query query)
    {
      this.typeRef = typeRef;
      this.future = future;
      this.url = url;
      this.query = query;
      jp = null;
    }

    @Override
    public boolean hasNext()
    {
      init();

      if (jp.isClosed()) {
        return false;
      }
      if (jp.getCurrentToken() == JsonToken.END_ARRAY) {
        CloseQuietly.close(jp);
        return false;
      }

      return true;
    }

    @Override
    public R next()
    {
      init();

      try {
        final R retVal = objectCodec.readValue(jp, typeRef);
        jp.nextToken();
        return retVal;
      }
      catch (IOException e) {
        throw Throwables.propagate(e);
      }
    }

    @Override
    public void remove()
    {
      throw new UnsupportedOperationException();
    }

    private void init()
    {
      if (jp == null) {
        try {
          InputStream is = future.get();
          if (is == null) {
            throw  new IOException(String.format("query[%s] url[%s] timed out", query, url));
          } else {
            jp = DruidStorageHandlerUtils.SMILE_MAPPER.getFactory().createParser(is);
          }
          final JsonToken nextToken = jp.nextToken();
          if (nextToken == JsonToken.START_OBJECT) {
            QueryInterruptedException cause = jp.getCodec().readValue(jp, QueryInterruptedException.class);
            throw new QueryInterruptedException(cause);
          } else if (nextToken != JsonToken.START_ARRAY) {
            throw new IAE("Next token wasn't a START_ARRAY, was[%s] from url [%s]", jp.getCurrentToken(), url);
          } else {
            jp.nextToken();
            objectCodec = jp.getCodec();
          }
        }
        catch (IOException | InterruptedException | ExecutionException e) {
          throw new RE(
                  e,
                  "Failure getting results for query[%s] url[%s] because of [%s]",
                  query.getId(),
                  url,
                  e.getMessage()
          );
        }
      }
    }

    @Override
    public void close() throws IOException
    {
      if (jp != null) {
        jp.close();
      }
    }
  }


}
