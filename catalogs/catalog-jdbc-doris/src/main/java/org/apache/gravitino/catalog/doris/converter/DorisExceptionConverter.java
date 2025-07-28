/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.gravitino.catalog.doris.converter;

import com.google.common.annotations.VisibleForTesting;
import java.sql.SQLException;
import java.util.List;
import java.util.function.Predicate;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.gravitino.catalog.jdbc.converter.JdbcExceptionConverter;
import org.apache.gravitino.exceptions.ConnectionFailedException;
import org.apache.gravitino.exceptions.GravitinoRuntimeException;
import org.apache.gravitino.exceptions.NoSuchColumnException;
import org.apache.gravitino.exceptions.NoSuchPartitionException;
import org.apache.gravitino.exceptions.NoSuchSchemaException;
import org.apache.gravitino.exceptions.NoSuchTableException;
import org.apache.gravitino.exceptions.PartitionAlreadyExistsException;
import org.apache.gravitino.exceptions.SchemaAlreadyExistsException;
import org.apache.gravitino.exceptions.TableAlreadyExistsException;
import org.apache.gravitino.exceptions.UnauthorizedException;

/** Exception converter to Apache Gravitino exception for Apache Doris. */
public class DorisExceptionConverter extends JdbcExceptionConverter {

  // see: https://doris.apache.org/docs/admin-manual/maint-monitor/doris-error-code/
  @VisibleForTesting static final int CODE_DATABASE_EXISTS = 1007;

  static final int CODE_TABLE_EXISTS = 1050;
  static final int CODE_NO_SUCH_SCHEMA = 1049;
  static final int CODE_DATABASE_NOT_EXISTS = 1008;
  static final int CODE_UNKNOWN_DATABASE = 1049;
  static final int CODE_NO_SUCH_TABLE = 1051;
  static final int CODE_UNAUTHORIZED = 1045;
  static final int CODE_NO_SUCH_COLUMN = 1054;
  static final int CODE_OTHER = 1105;
  static final int CODE_DELETE_NON_EXISTING_PARTITION = 1507;
  static final int CODE_PARTITION_ALREADY_EXISTS = 1517;

  private static final List<Pair<Predicate<String>, Integer>> errorMessageToErrorCodeList =
      List.of(
          Pair.of(
              msg ->
                  msg.contains("detailMessage = Can't create database")
                      && msg.contains("database exists"),
              1007),
          Pair.of(
              msg ->
                  msg.contains("detailMessage = Can't drop database")
                      && msg.contains("database doesn't exist"),
              1008),
          Pair.of(msg -> msg.contains("detailMessage = Unknown database"), 1049),
          Pair.of(msg -> msg.contains("detailMessage = Unknown table"), 1051),
          Pair.of(msg -> msg.contains("detailMessage = Error in list of partitions to"), 1507),
          Pair.of(msg -> msg.contains("detailMessage = Duplicate partition name"), 1517));

  @SuppressWarnings("FormatStringAnnotation")
  @Override
  public GravitinoRuntimeException toGravitinoException(SQLException se) {
    int errorCode = se.getErrorCode();
    if (errorCode == CODE_OTHER) {
      errorCode = getErrorCodeFromMessage(se.getMessage());
    }

    switch (errorCode) {
      case CODE_DATABASE_EXISTS:
        return new SchemaAlreadyExistsException(se, se.getMessage());
      case CODE_TABLE_EXISTS:
        return new TableAlreadyExistsException(se, se.getMessage());
      case CODE_DATABASE_NOT_EXISTS:
      case CODE_UNKNOWN_DATABASE:
        return new NoSuchSchemaException(se, se.getMessage());
      case CODE_NO_SUCH_TABLE:
        return new NoSuchTableException(se, se.getMessage());
      case CODE_UNAUTHORIZED:
        return new UnauthorizedException(se, se.getMessage());
      case CODE_NO_SUCH_COLUMN:
        return new NoSuchColumnException(se, se.getMessage());
      case CODE_DELETE_NON_EXISTING_PARTITION:
        return new NoSuchPartitionException(se, se.getMessage());
      case CODE_PARTITION_ALREADY_EXISTS:
        return new PartitionAlreadyExistsException(se, se.getMessage());
      default:
        if (se.getMessage() != null && se.getMessage().contains("Access denied")) {
          return new ConnectionFailedException(se, se.getMessage());
        }
        return new GravitinoRuntimeException(se, se.getMessage());
    }
  }

  @VisibleForTesting
  static int getErrorCodeFromMessage(String message) {
    if (message.isEmpty()) {
      return CODE_OTHER;
    }

    for (Pair<Predicate<String>, Integer> errorMessageCodePair : errorMessageToErrorCodeList) {
      boolean doesMessageContainErrorSubstring = errorMessageCodePair.getLeft().test(message);
      if (doesMessageContainErrorSubstring) return errorMessageCodePair.getRight();
    }

    return CODE_OTHER;
  }
}
