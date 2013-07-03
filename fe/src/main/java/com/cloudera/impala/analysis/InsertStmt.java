// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.analysis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import com.cloudera.impala.catalog.Catalog;
import com.cloudera.impala.catalog.Column;
import com.cloudera.impala.catalog.Db;
import com.cloudera.impala.catalog.Db.TableLoadingException;
import com.cloudera.impala.catalog.HBaseTable;
import com.cloudera.impala.catalog.PrimitiveType;
import com.cloudera.impala.catalog.Table;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.InternalException;
import com.cloudera.impala.planner.DataSink;
import com.google.common.base.Preconditions;

/**
 * Representation of a single insert statement, including the select statement
 * whose results are to be inserted.
 *
 */
public class InsertStmt extends ParseNodeBase {
  // Target table name as seen by the parser
  private final TableName originalTableName;
  // Target table into which to insert. May be qualified by analyze()
  private TableName targetTableName;
  // Differentiates between INSERT INTO and INSERT OVERWRITE.
  private final boolean overwrite;
  // List of column:value elements from the PARTITION (...) clause.
  // Set to null if no partition was given.
  private final List<PartitionKeyValue> partitionKeyValues;
  // Select or union whose results are to be inserted.
  private final QueryStmt queryStmt;
  // Set in analyze(). Contains metadata of target table to determine type of sink.
  private Table table;
  // Set in analyze(). Exprs corresponding to the partitionKeyValues,
  private final List<Expr> partitionKeyExprs = new ArrayList<Expr>();
  // True if this InsertStmt is the top level query from an EXPLAIN <query>
  private boolean isExplain = false;

  public InsertStmt(TableName targetTable, boolean overwrite,
      List<PartitionKeyValue> partitionKeyValues, QueryStmt queryStmt) {
    this.targetTableName = targetTable;
    this.originalTableName = targetTableName;
    this.overwrite = overwrite;
    this.partitionKeyValues = partitionKeyValues;
    this.queryStmt = queryStmt;
    table = null;
  }

  @Override
  public void analyze(Analyzer analyzer) throws AnalysisException, InternalException {
    queryStmt.analyze(analyzer);
    List<Expr> selectListExprs = queryStmt.getResultExprs();
    Catalog catalog = analyzer.getCatalog();

    if (!targetTableName.isFullyQualified()) {
      this.targetTableName = new TableName(analyzer.getDefaultDb(),
                                           targetTableName.getTbl());
    }

    try {
      Db db = catalog.getDb(targetTableName.getDb());
      if (db == null) {
        throw new AnalysisException("Unknown db: '" + targetTableName.getDb() + "'.");
      }
      table = catalog.getDb(targetTableName.getDb()).getTable(targetTableName.getTbl());
    } catch (TableLoadingException e) {
      throw new AnalysisException("Failed to load metadata for table: "
          + targetTableName.getTbl(), e);
    }
    if (table == null) {
      throw new AnalysisException("Unknown table: '" + targetTableName.toString() +
          "' in db: '" + targetTableName.getDb() + "'.");
    }

    // Add target table to descriptor table.
    analyzer.getDescTbl().addReferencedTable(table);

    boolean isHBaseTable = (table instanceof HBaseTable);
    int numClusteringCols = isHBaseTable ? 0 : table.getNumClusteringCols();
    int numDynamicPartKeys = 0;

    if (isHBaseTable && overwrite) {
      throw new AnalysisException("HBase doesn't have a way to perform INSERT OVERWRITE");
    }

    // Make sure there is a set of partitionKeys when the table below
    // indicates they are needed.
    //
    //  Table Type | numClusteringCols  | Expecting PartionKeys
    //  -----------+--------------------+------------------------
    //  HDFS       | 1+                 | Yes
    //  HDFS       | 0                  | No
    //  HBase      | 1+                 | No

    if (partitionKeyValues == null) {
      if (numClusteringCols != 0) {
        // Partitioned table but no partition clause.
        throw new AnalysisException("No PARTITION clause given for INSERT into " +
            "partitioned table '" + targetTableName.getTbl() + "'.");
      }
    } else {
      if (numClusteringCols == 0) {
        // Unpartitioned table, but INSERT has PARTITION clause
        throw new AnalysisException("PARTITION clause is only valid for INSERT into " +
            "partitioned table. '" + targetTableName.getTbl() + "' is not partitioned");
      }

      // Make sure static partition key values only contain const exprs.
      for (PartitionKeyValue kv: partitionKeyValues) {
        kv.analyze(analyzer);
      }

      // Check that the partition clause mentions all the table's partitioning
      // columns, and that no non-partition columns are mentioned in the
      // partition clause.
      checkPartitionClauseCompleteness();

      // Check that all dynamic partition keys are at the end of the selectListExprs.
      numDynamicPartKeys = fillPartitionKeyExprs();

      // make sure we have stats for partitionKeyExprs
      for (Expr expr: partitionKeyExprs) {
        expr.analyze(analyzer);
      }
    }

    // Check union compatibility, ignoring partitioning columns for dynamic partitions.
    checkUnionCompatibility(table, selectListExprs, numDynamicPartKeys);
  }

  /**
   * Checks whether all partitioning columns in table are mentioned in
   * partitionKeyValues, and that all partitionKeyValues have a match in table.
   *
   * @throws AnalysisException
   *           If the partitionKeyValues don't mention all partitioning columns in
   *           table, or if they mention extra columns.
   */
  private void checkPartitionClauseCompleteness()
      throws AnalysisException {
    List<Column> columns = table.getColumns();
    int numClusteringCols = table.getNumClusteringCols();
    // Copy the partition key values a temporary list.
    // We remove items as we match them against partitioning columns in the table.
    List<PartitionKeyValue> unmatchedPartKeyVals = new LinkedList<PartitionKeyValue>();
    unmatchedPartKeyVals.addAll(partitionKeyValues);
    // Check that all partitioning columns were mentioned in the partition clause.
    // Remove matching items from unmatchedPartKeyVals
    // to detect superfluous columns in the partition clause.
    for (int i = 0; i < numClusteringCols; ++i) {
      PartitionKeyValue matchingPartKeyVal = null;
      Iterator<PartitionKeyValue> clauseIter = unmatchedPartKeyVals.iterator();
      while (clauseIter.hasNext()) {
        PartitionKeyValue pkv = clauseIter.next();
        if (pkv.getColName().equals(columns.get(i).getName())) {
          matchingPartKeyVal = pkv;
          clauseIter.remove();
          break;
        }
      }
      if (matchingPartKeyVal == null) {
        throw new AnalysisException("Missing partition column '"
            + columns.get(i).getName() + "' from PARTITION clause.");
      }
    }
    // All partitioning columns of the table were matched.
    // Check for superfluous columns in the partition clause.
    if (!unmatchedPartKeyVals.isEmpty()) {
      StringBuilder strBuilder = new StringBuilder();
      for (PartitionKeyValue pkv : unmatchedPartKeyVals) {
        strBuilder.append(pkv.getColName() + ",");
      }
      strBuilder.deleteCharAt(strBuilder.length() - 1);
      throw new AnalysisException("Superfluous columns in PARTITION clause: "
          + strBuilder.toString() + ".");
    }
  }

  /**
   * Fills the partitionKeyExprs class member, by positionally
   * matching the dynamic partition keys
   * against the last numDynamicPartKeys selectListExprs.
   * If necessary, adds casts to the selectListExprs to make them compatible
   * with the type of the corresponding partitioning column.
   *
   * @return Number of dynamic partition keys.
   * @throws AnalysisException
   *           If not all dynamic partition keys are mentioned in the selectListExprs.
   */
  private int fillPartitionKeyExprs() throws AnalysisException {
    // Count the number of dynamic partition keys.
    int numDynamicPartKeys = 0;
    for (PartitionKeyValue pkv : partitionKeyValues) {
      if (pkv.isDynamic()) {
        ++numDynamicPartKeys;
      }
    }
    List<Expr> selectListExprs = queryStmt.getResultExprs();
    // Position of selectListExpr corresponding to the next dynamic partition column.
    int exprMatchPos = table.getColumns().size() - table.getNumClusteringCols();
    // Temporary lists of partition key exprs and names in an arbitrary order.
    List<Expr> tmpPartitionKeyExprs = new ArrayList<Expr>();
    List<String> tmpPartitionKeyNames = new ArrayList<String>();
    for (PartitionKeyValue pkv : partitionKeyValues) {
      Expr partitionColValue;
      if (pkv.isStatic()) {
        partitionColValue = pkv.getValue();
      } else {
        if (exprMatchPos >= selectListExprs.size()) {
          throw new AnalysisException("No matching select list item found for "
              + "dynamic partition '" + pkv.getColName() + "'.\n"
              + "The select list items corresponding to dynamic partition "
              + "keys must be at the end of the select list.");
        }
        partitionColValue = selectListExprs.get(exprMatchPos++);
      }
      Column tableColumn = table.getColumn(pkv.getColName());
      Expr compatibleExpr = checkTypeCompatibility(tableColumn, partitionColValue);
      tmpPartitionKeyExprs.add(compatibleExpr);
      tmpPartitionKeyNames.add(pkv.getColName());
    }
    // Reorder the partition key exprs and names to be consistent
    // with the target table declaration.
    // We need those exprs in the original order to create the
    // corresponding Hdfs folder structure correctly.

    int numClusteringCols = table.getNumClusteringCols();
    for (int i = 0; i < numClusteringCols; ++i) {
      Column c = table.getColumns().get(i);
      for (int j = 0; j < tmpPartitionKeyNames.size(); ++j) {
        if (c.getName().equals(tmpPartitionKeyNames.get(j))) {
          partitionKeyExprs.add(tmpPartitionKeyExprs.get(j));
          break;
        }
      }
    }

    Preconditions.checkState(partitionKeyExprs.size() == numClusteringCols);
    return numDynamicPartKeys;
  }

  /**
   * Checks for union compatibility of a table and a list of exprs.
   * May cast the exprs to higher precision types,
   * if necessary, to make them compatible with their corresponding table columns.
   *
   * @param table
   *          Table from the metadata
   * @param selectListExprs
   *          In/Out: List of expressions from a select statement.
   *          Possibly modified with casts.
   * @param numDynamicPartKeys
   *          The number of dynamic partition keys. We assume that the partition keys
   *          have a verified match at the end of the select list.
   * @throws AnalysisException
   *           If the columns and exprs are not union compatible,
   *           or if making them union compatible
   *           would lose precision in at least one column.
   */
  private void checkUnionCompatibility(Table table, List<Expr> selectListExprs,
      int numDynamicPartKeys)
      throws AnalysisException {
    List<Column> columns = table.getColumns();

    // For writes to hbase every query node can write to any part of
    // the row key space. So use 0 as the numClusteringCols.
    int numClusteringCols =
        (table instanceof HBaseTable) ? 0 : table.getNumClusteringCols();

    int numNonClusteringCols = columns.size() - numClusteringCols;
    if (numNonClusteringCols != selectListExprs.size() - numDynamicPartKeys) {
      throw new AnalysisException("Target table '" + targetTableName.getTbl()
          + "' and result of select statement are not union compatible.\n"
          + "Target table expects "
          + numNonClusteringCols + " columns but the select statement returns "
          + (selectListExprs.size() - numDynamicPartKeys) + ".");
    }

    for (int i = numClusteringCols; i < columns.size(); ++i) {
      int selectListIndex = i - numClusteringCols;
      // Check for compatible type, and add casts to the selectListExprs if necessary.
      Expr expr = checkTypeCompatibility(columns.get(i),
          selectListExprs.get(selectListIndex));
      selectListExprs.set(selectListIndex, expr);
    }
  }

  /**
   * Checks for type compatibility of column and expr.
   * Returns compatible (possibly cast) expr.
   *
   * @param column
   *          Table column.
   * @param expr
   *          Expr to be checked for type compatibility with column,
   * @return
   *         Possibly cast compatible expr.
   * @throws AnalysisException
   *           If the column and expr type are incompatible, or if casting the
   *           expr would lead to loss of precision.
   */
  private Expr checkTypeCompatibility(Column column, Expr expr)
      throws AnalysisException {
    // Check for compatible type, and add casts to the selectListExprs if necessary.
    // We don't allow casting to a lower precision type.
    PrimitiveType colType = column.getType();
    PrimitiveType exprType = expr.getType();
    // Trivially compatible.
    if (colType == exprType) {
      return expr;
    }
    PrimitiveType compatibleType =
        PrimitiveType.getAssignmentCompatibleType(colType, exprType);
    // Incompatible types.
    if (!compatibleType.isValid()) {
      throw new AnalysisException("Target table '" + targetTableName.getTbl()
          + "' and result of select statement are not union compatible.\n"
          + "Incompatible types '" + colType.toString() + "' and '"
          + exprType.toString() + "' in column '" + expr.toSql() + "'.");
    }
    // Loss of precision when inserting into the table.
    if (compatibleType != colType && !compatibleType.isNull()) {
      throw new AnalysisException("Inserting into target table '"
          + targetTableName.getTbl() + "' may result in loss of precision.\n"
          + "Would need to cast '"
          + expr.toSql() + "' to '"
          + colType.toString() + "'.");
    }
    // Add a cast to the selectListExpr to the higher type.
    Expr castExpr = expr.castTo(compatibleType);
    return castExpr;
  }

  public TableName getTargetTableName() {
    return targetTableName;
  }

  public Table getTargetTable() {
    return table;
  }

  public boolean isOverwrite() {
    return overwrite;
  }

  public QueryStmt getQueryStmt() {
    return queryStmt;
  }

  public List<PartitionKeyValue> getPartitionList() {
    return partitionKeyValues;
  }

  public List<Expr> getPartitionKeyExprs() {
    return partitionKeyExprs;
  }

  public DataSink createDataSink() {
    // analyze() must have been called before.
    Preconditions.checkState(table != null);
    return table.createDataSink(partitionKeyExprs, overwrite);
  }

  @Override
  public String toSql() {
    StringBuilder strBuilder = new StringBuilder();
    strBuilder.append("INSERT ");
    if (overwrite) {
      strBuilder.append("OVERWRITE ");
    } else {
      strBuilder.append("INTO ");
    }
    strBuilder.append("TABLE " + originalTableName + " ");
    if (partitionKeyValues != null) {
      strBuilder.append("PARTITION (");
      for (int i = 0; i < partitionKeyValues.size(); ++i) {
        PartitionKeyValue pkv = partitionKeyValues.get(i);
        strBuilder.append(pkv.getColName());
        if (pkv.getValue() != null) {
          strBuilder.append("=" + pkv.getValue().toSql());
        }
        strBuilder.append((i+1 != partitionKeyValues.size()) ? ", " : "");
      }
      strBuilder.append(") ");
    }
    strBuilder.append(queryStmt.toSql());
    return strBuilder.toString();
  }

  public void setIsExplain(boolean isExplain) {
    this.isExplain = isExplain;
  }

  public boolean isExplain() {
    return isExplain;
  }
}
