/*
 * Copyright 2016 Confluent Inc.
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
 */

package io.confluent.connect.jdbc.sink;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import io.confluent.connect.jdbc.dialect.DatabaseDialect;
import io.confluent.connect.jdbc.dialect.DatabaseDialect.StatementBinder;
import io.confluent.connect.jdbc.sink.metadata.FieldsMetadata;
import io.confluent.connect.jdbc.sink.metadata.SchemaPair;
import io.confluent.connect.jdbc.util.ColumnId;
import io.confluent.connect.jdbc.util.TableId;

public class BufferedRecords {
  private static final Logger log = LoggerFactory.getLogger(BufferedRecords.class);

  private final TableId tableId;
  private final JdbcSinkConfig config;
  private final DatabaseDialect dbDialect;
  private final DbStructure dbStructure;
  private final Connection connection;

  private List<SinkRecord> records = new ArrayList<>();
  private SchemaPair currentSchemaPair;
  private FieldsMetadata fieldsMetadata;
  private PreparedStatement preparedStatement;
  private StatementBinder preparedStatementBinder;

  public BufferedRecords(
      JdbcSinkConfig config,
      TableId tableId,
      DatabaseDialect dbDialect,
      DbStructure dbStructure,
      Connection connection
  ) {
    this.tableId = tableId;
    this.config = config;
    this.dbDialect = dbDialect;
    this.dbStructure = dbStructure;
    this.connection = connection;
  }

  public List<SinkRecord> add(SinkRecord record) throws SQLException {
    final SchemaPair schemaPair = new SchemaPair(
        record.keySchema(),
        record.valueSchema()
    );

    if (currentSchemaPair == null) {
      currentSchemaPair = schemaPair;

      // re-initialize everything that depends on the record schema
      fieldsMetadata = FieldsMetadata.extract(
              tableId.tableName(),
              config.pkMode,
              config.pkFields,
              config.fieldsWhitelist,
              currentSchemaPair
      );

      String parameterizedRawSql;

      //Tombstone Record
      if (currentSchemaPair.valueSchema == null && config.deleteEnabled) {
        parameterizedRawSql = getDeleteSql();
      } else {
        dbStructure.createOrAmendIfNecessary(
                 config,
                 connection,
                 tableId,
                 fieldsMetadata
        );
        parameterizedRawSql = getInsertSql();
      }

      log.debug(
               "{} sql: {}",
               config.insertMode,
               parameterizedRawSql
      );
      close();
      preparedStatement = connection.prepareStatement(parameterizedRawSql);
      preparedStatementBinder = dbDialect.statementBinder(
               preparedStatement,
               config.pkMode,
               schemaPair,
               fieldsMetadata,
               config.insertMode,
               config.deleteEnabled
      );
    }

    final List<SinkRecord> flushed;
    if (currentSchemaPair.equals(schemaPair)) {
      // Continue with current batch state
      records.add(record);
      if (records.size() >= config.batchSize) {
        flushed = flush();
      } else {
        flushed = Collections.emptyList();
      }
    } else {
      // Each batch needs to have the same SchemaPair, so get the buffered records out, reset
      // state and re-attempt the add
      flushed = flush();
      currentSchemaPair = null;
      flushed.addAll(add(record));
    }
    return flushed;
  }

  public List<SinkRecord> flush() throws SQLException {
    if (records.isEmpty()) {
      return new ArrayList<>();
    }
    for (SinkRecord record : records) {
      preparedStatementBinder.bindRecord(record);
    }
    int totalUpdateCount = 0;
    boolean successNoInfo = false;
    for (int updateCount : preparedStatement.executeBatch()) {
      log.info("Update Cnt {}", updateCount);
      if (updateCount == Statement.SUCCESS_NO_INFO) {
        successNoInfo = true;
        continue;
      }
      // In case multiple deletes passed, delete becomes idempotent
      if (currentSchemaPair.valueSchema == null
              && config.deleteEnabled
              && updateCount == 0) {
        updateCount = 1;
      }
      totalUpdateCount += updateCount;
    }
    if (totalUpdateCount != records.size() && !successNoInfo) {
      if (currentSchemaPair.valueSchema == null
              && config.deleteEnabled) {
        throw new ConnectException(String.format(
                "Delete count (%d) did not sum up to total number of records deleted (%d)",
                totalUpdateCount,
                records.size()
        ));
      } else {
        switch (config.insertMode) {
          case INSERT:
            throw new ConnectException(String.format(
                    "Update count (%d) did not sum up to total number of records inserted (%d)",
                    totalUpdateCount,
                    records.size()
            ));
          case UPSERT:
          case UPDATE:
            log.trace(
                    "{} records:{} resulting in in totalUpdateCount:{}",
                    config.insertMode,
                    records.size(),
                    totalUpdateCount
            );
            break;
          default:
            throw new ConnectException("Unknown insert mode: " + config.insertMode);
        }
      }
    }
    if (successNoInfo) {
      log.info(
          "{} records:{} , but no count of the number of rows it affected is available",
          config.insertMode,
          records.size()
      );
    }

    final List<SinkRecord> flushedRecords = records;
    records = new ArrayList<>();
    return flushedRecords;
  }

  public void close() throws SQLException {
    if (preparedStatement != null) {
      preparedStatement.close();
      preparedStatement = null;
    }
  }

  private String getInsertSql() {
    switch (config.insertMode) {
      case INSERT:
        return dbDialect.buildInsertStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      case UPSERT:
        if (fieldsMetadata.keyFieldNames.isEmpty()) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode requires key field names to be known, check the"
              + " primary key configuration",
              tableId
          ));
        }
        try {
          return dbDialect.buildUpsertQueryStatement(
              tableId,
              asColumns(fieldsMetadata.keyFieldNames),
              asColumns(fieldsMetadata.nonKeyFieldNames)
          );
        } catch (UnsupportedOperationException e) {
          throw new ConnectException(String.format(
              "Write to table '%s' in UPSERT mode is not supported with the %s dialect.",
              tableId,
              dbDialect.name()
          ));
        }
      case UPDATE:
        return dbDialect.buildUpdateStatement(
            tableId,
            asColumns(fieldsMetadata.keyFieldNames),
            asColumns(fieldsMetadata.nonKeyFieldNames)
        );
      default:
        throw new ConnectException("Invalid insert mode");
    }
  }

  private String getDeleteSql() {
    String sql = null;
    if (config.deleteEnabled) {
      switch (config.pkMode) {
        case NONE:
        case KAFKA:
        case RECORD_VALUE:
          throw new ConnectException("Deletes are only supported for pk.mode record_key");
        case RECORD_KEY:
          if (fieldsMetadata.keyFieldNames.isEmpty()) {
            throw new ConnectException("Require primary keys to support delete");
          }
          sql = dbDialect.getDelete(tableId, asColumns(fieldsMetadata.keyFieldNames));
          break;
        default:
          throw new ConnectException("Invalid pk.mode");
      }
    }
    return sql;
  }

  private Collection<ColumnId> asColumns(Collection<String> names) {
    return names.stream()
                .map(name -> new ColumnId(tableId, name))
                .collect(Collectors.toList());
  }
}
