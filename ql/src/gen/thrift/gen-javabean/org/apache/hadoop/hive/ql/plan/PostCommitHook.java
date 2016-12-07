/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.QBParseInfo;

@Explain(displayName = "Post commit hook operator", explainLevels = { Explain.Level.USER, Explain.Level.DEFAULT, Explain.Level.EXTENDED })
public class PostCommitHook extends DDLDesc{
  private final QBParseInfo parseInfo;
  private final Path path;
  private final Table table;


  public PostCommitHook(Table dest_tab, Path path,
          QBParseInfo parseInfo
  ) {
    this.table = dest_tab;
    this.path = path;
    this.parseInfo = parseInfo;
  }

  public Path getPath() {
    return path;
  }

  public Table getTable() {
    return table;
  }

  public boolean isOverwrite()
  {
    return !parseInfo.isInsertIntoTable(table.getTableName());
  }
}
