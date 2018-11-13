/**
 * Copyright 2015 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

package io.confluent.connect.jdbc.source;

import org.apache.kafka.connect.source.SourceRecord;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.util.TableId;

/**
 * TableQuerier executes queries against a specific table. Implementations handle different types
 * of queries: periodic bulk loading, incremental loads using auto incrementing IDs, incremental
 * loads using timestamps, etc.
 */
abstract class TableQuerier implements Comparable<TableQuerier> {
  public enum QueryMode {
    TABLE, // Copying whole tables, with queries constructed automatically
    QUERY // User-specified query
  }

  protected final DatabaseDialect dialect;
  protected final QueryMode mode;
  protected final String query;
  protected final String topicPrefix;
  protected final TableId tableId;

  // Mutable state

  protected long lastUpdate;
  protected PreparedStatement stmt;
  protected ResultSet resultSet;
  protected SchemaMapping schemaMapping;
  protected boolean isBootstrapEnabled;

  public TableQuerier(
      DatabaseDialect dialect,
      QueryMode mode,
      String nameOrQuery,
      String topicPrefix,
      boolean isBoostrapEnabled
  ) {
    this.dialect = dialect;
    this.mode = mode;
    this.tableId = mode.equals(QueryMode.TABLE) ? dialect.parseTableIdentifier(nameOrQuery) : null;
    this.query = mode.equals(QueryMode.QUERY) ? nameOrQuery : null;
    this.topicPrefix = topicPrefix;
    this.lastUpdate = 0;
    this.isBootstrapEnabled = isBoostrapEnabled;
  }

  public long getLastUpdate() {
    return lastUpdate;
  }

  public PreparedStatement getOrCreatePreparedStatement(Connection db) throws SQLException {
    // If bootstrap is enabled, re-create stmt
    if (stmt != null) {
      if (!isBootstrapEnabled) {
        return stmt;
      } else {
        stmt.close();
      }

    }
    createPreparedStatement(db);
    return stmt;
  }

  protected abstract void createPreparedStatement(Connection db) throws SQLException;

  public boolean querying() {
    return resultSet != null;
  }

  public void maybeStartQuery(Connection db) throws SQLException {
    if (resultSet == null) {
      stmt = getOrCreatePreparedStatement(db);
      resultSet = executeQuery();
      String schemaName = tableId != null ? tableId.tableName() : null; // backwards compatible
      schemaMapping = SchemaMapping.create(schemaName, resultSet.getMetaData(), dialect);
    }
  }

  protected abstract ResultSet executeQuery() throws SQLException;

  public boolean next() throws SQLException {
    return resultSet.next();
  }

  public abstract SourceRecord extractRecord() throws SQLException;

  public void reset(long now) {
    closeResultSetQuietly();
    closeStatementQuietly();
    // TODO: Can we cache this and quickly check that it's identical for the next query
    // instead of constructing from scratch since it's almost always the same
    schemaMapping = null;
    lastUpdate = now;
  }

  private void closeStatementQuietly() {
    if (stmt != null) {
      try {
        stmt.close();
      } catch (SQLException ignored) {
        // intentionally ignored
      }
    }
    stmt = null;
  }

  private void closeResultSetQuietly() {
    if (resultSet != null) {
      try {
        resultSet.close();
      } catch (SQLException ignored) {
        // intentionally ignored
      }
    }
    resultSet = null;
  }

  @Override
  public int compareTo(TableQuerier other) {
    if (this.lastUpdate < other.lastUpdate) {
      return -1;
    } else if (this.lastUpdate > other.lastUpdate) {
      return 1;
    } else {
      return this.tableId.compareTo(other.tableId);
    }
  }
}
