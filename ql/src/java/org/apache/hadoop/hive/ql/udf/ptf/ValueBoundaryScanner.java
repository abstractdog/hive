/*
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

package org.apache.hadoop.hive.ql.udf.ptf;

import java.util.Map;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.exec.BoundaryCache;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.plan.ptf.BoundaryDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderDef;
import org.apache.hadoop.hive.ql.plan.ptf.OrderExpressionDef;
import org.apache.hadoop.hive.ql.plan.ptf.WindowFrameDef;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public abstract class ValueBoundaryScanner {
  BoundaryDef start, end;
  protected final boolean nullsLast;

  public ValueBoundaryScanner(BoundaryDef start, BoundaryDef end, boolean nullsLast) {
    this.start = start;
    this.end = end;
    this.nullsLast = nullsLast;
  }

  public abstract Object computeValue(Object row) throws HiveException;

  /**
   * Checks if the distance of v2 to v1 is greater than the given amt.
   * @return True if the value of v1 - v2 is greater than amt or either value is null.
   */
  public abstract boolean isDistanceGreater(Object v1, Object v2, int amt);

  /**
   * Checks if the values of v1 or v2 are the same.
   * @return True if both values are the same or both are nulls.
   */
  public abstract boolean isEqual(Object v1, Object v2);

  public abstract int computeStart(int rowIdx, PTFPartition p) throws HiveException;

  public abstract int computeEnd(int rowIdx, PTFPartition p) throws HiveException;

  /**
   * Checks and maintains cache content - optimizes cache window to always be around current row
   * thereby makes it follow the current progress.
   * @param rowIdx current row
   * @param p current partition for the PTF operator
   * @throws HiveException
   */
  public void handleCache(int rowIdx, PTFPartition p) throws HiveException {
    BoundaryCache cache = p.getBoundaryCache();
    if (cache == null) {
      return;
    }

    //No need to setup/fill cache.
    if (start.isUnbounded() && end.isUnbounded()) {
      return;
    }

    //Start of partition.
    if (rowIdx == 0) {
      cache.clear();
    }
    if (cache.isComplete()) {
      return;
    }
    if (cache.isEmpty()) {
      fillCacheUntilEndOrFull(rowIdx, p);
      return;
    }

    if (start.isPreceding()) {
      if (start.isUnbounded()) {
        if (end.isPreceding()) {
          //We can wait with cache eviction until we're at the end of currently known ranges.
          Map.Entry<Integer, Object> maxEntry = cache.getMaxEntry();
          if (maxEntry != null && maxEntry.getKey() <= rowIdx) {
            cache.evictOne();
          }
        } else {
          //Starting from current row, all previous ranges can be evicted.
          checkIfCacheCanEvict(rowIdx, p, true);
        }
      } else {
        //We either evict when we're at the end of currently known ranges, or if not there yet and
        // END is of FOLLOWING type: we should remove ranges preceding the current range beginning.
        Map.Entry<Integer, Object> maxEntry = cache.getMaxEntry();
        if (maxEntry != null && maxEntry.getKey() <= rowIdx) {
          cache.evictOne();
        } else if (end.isFollowing()) {
          int startIdx = computeStart(rowIdx, p);
          checkIfCacheCanEvict(startIdx - 1, p, true);
        }
      }
    }

    if (start.isCurrentRow()) {
      //Starting from current row, all previous ranges before the previous range can be evicted.
      checkIfCacheCanEvict(rowIdx, p, false);
    }
    if (start.isFollowing()) {
      //Starting from current row, all previous ranges can be evicted.
      checkIfCacheCanEvict(rowIdx, p, true);
    }

    fillCacheUntilEndOrFull(rowIdx, p);
  }

  /**
   * Retrieves the range for rowIdx, then removes all previous range entries before it.
   * @param rowIdx row index.
   * @param p partition.
   * @param willScanFwd false: removal is started only from the previous previous range.
   */
  private void checkIfCacheCanEvict(int rowIdx, PTFPartition p, boolean willScanFwd) {
    BoundaryCache cache = p.getBoundaryCache();
    if (cache == null) {
      return;
    }
    Map.Entry<Integer, Object> floorEntry = cache.floorEntry(rowIdx);
    if (floorEntry != null) {
      floorEntry = cache.floorEntry(floorEntry.getKey() - 1);
      if (floorEntry != null) {
        if (willScanFwd) {
          cache.evictThisAndAllBefore(floorEntry.getKey());
        } else {
          floorEntry = cache.floorEntry(floorEntry.getKey() - 1);
          if (floorEntry != null) {
            cache.evictThisAndAllBefore(floorEntry.getKey());
          }
        }
      }
    }
  }

  /**
   * Inserts values into cache starting from rowIdx in the current partition p. Stops if cache
   * reaches its maximum size or we get out of rows in p.
   * @param rowIdx
   * @param p
   * @throws HiveException
   */
  private void fillCacheUntilEndOrFull(int rowIdx, PTFPartition p) throws HiveException {
    BoundaryCache cache = p.getBoundaryCache();
    if (cache == null || p.size() <= 0) {
      return;
    }

    Object rowVal = null;

    //If we continue building cache
    Map.Entry<Integer, Object> ceilingEntry = cache.getMaxEntry();
    if (ceilingEntry != null) {
      rowIdx = ceilingEntry.getKey();
      rowVal = ceilingEntry.getValue();
      ++rowIdx;
    }

    Object lastRowVal = rowVal;

    while (rowIdx < p.size() && !cache.isFull()) {
      rowVal = computeValue(p.getAt(rowIdx));
      if (!isEqual(rowVal, lastRowVal)){
        cache.put(rowIdx, rowVal);
      }
      lastRowVal = rowVal;
      ++rowIdx;

    }
    //Signaling end of all rows in a partition
    if (cache.putIfNotFull(rowIdx, null)) {
      cache.setComplete(true);
    }
  }

  /**
   * Uses cache content to jump backwards if possible. If not, it steps one back.
   * @param r
   * @param p
   * @return pair of (row we stepped/jumped onto ; row value at this position)
   * @throws HiveException
   */
  protected Pair<Integer, Object> skipOrStepBack(int r, PTFPartition p)
          throws HiveException {
    Object rowVal = null;
    BoundaryCache cache = p.getBoundaryCache();

    Map.Entry<Integer, Object> floorEntry = null;
    Map.Entry<Integer, Object> ceilingEntry = null;

    if (cache != null) {
      floorEntry = cache.floorEntry(r);
      ceilingEntry = cache.ceilingEntry(r);
    }

    if (floorEntry != null && ceilingEntry != null) {
      r = floorEntry.getKey() - 1;
      floorEntry = cache.floorEntry(r);
      if (floorEntry != null) {
        rowVal = floorEntry.getValue();
      } else if (r >= 0){
        rowVal = computeValue(p.getAt(r));
      }
    } else {
      r--;
      if (r >= 0) {
        rowVal = computeValue(p.getAt(r));
      }
    }
    return new ImmutablePair<>(r, rowVal);
  }

  /**
   * Uses cache content to jump forward if possible. If not, it steps one forward.
   * @param r
   * @param p
   * @return pair of (row we stepped/jumped onto ; row value at this position)
   * @throws HiveException
   */
  protected Pair<Integer, Object> skipOrStepForward(int r, PTFPartition p)
          throws HiveException {
    Object rowVal = null;
    BoundaryCache cache = p.getBoundaryCache();

    Map.Entry<Integer, Object> floorEntry = null;
    Map.Entry<Integer, Object> ceilingEntry = null;

    if (cache != null) {
      floorEntry = cache.floorEntry(r);
      ceilingEntry = cache.ceilingEntry(r);
    }

    if (ceilingEntry != null && ceilingEntry.getKey().equals(r)){
      ceilingEntry = cache.ceilingEntry(r + 1);
    }
    if (floorEntry != null && ceilingEntry != null) {
      r = ceilingEntry.getKey();
      rowVal = ceilingEntry.getValue();
    } else {
      r++;
      if (r < p.size()) {
        rowVal = computeValue(p.getAt(r));
      }
    }
    return new ImmutablePair<>(r, rowVal);
  }

  /**
   * Uses cache to lookup row value. Computes it on the fly on cache miss.
   * @param r
   * @param p
   * @return row value.
   * @throws HiveException
   */
  protected Object computeValueUseCache(int r, PTFPartition p) throws HiveException {
    BoundaryCache cache = p.getBoundaryCache();

    Map.Entry<Integer, Object> floorEntry = null;
    Map.Entry<Integer, Object> ceilingEntry = null;

    if (cache != null) {
      floorEntry = cache.floorEntry(r);
      ceilingEntry = cache.ceilingEntry(r);
    }

    if (ceilingEntry != null && ceilingEntry.getKey().equals(r)){
      return ceilingEntry.getValue();
    }
    if (floorEntry != null && ceilingEntry != null) {
      return floorEntry.getValue();
    } else {
      return computeValue(p.getAt(r));
    }
  }

  public static ValueBoundaryScanner getScanner(WindowFrameDef winFrameDef, boolean nullsLast)
      throws HiveException {
    OrderDef orderDef = winFrameDef.getOrderDef();
    int numOrders = orderDef.getExpressions().size();
    if (numOrders != 1) {
      return new MultiValueBoundaryScanner(winFrameDef.getStart(), winFrameDef.getEnd(), orderDef,
          nullsLast);
    } else {
      return SingleValueBoundaryScanner.getScanner(winFrameDef.getStart(), winFrameDef.getEnd(),
          orderDef, nullsLast);
    }
  }
}

/*
 * - starting from the given rowIdx scan in the given direction until a row's expr
 * evaluates to an amt that crosses the 'amt' threshold specified in the BoundaryDef.
 */
abstract class SingleValueBoundaryScanner extends ValueBoundaryScanner {
  OrderExpressionDef expressionDef;

  public SingleValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, nullsLast);
    this.expressionDef = expressionDef;
  }

  /*
|  Use | Boundary1.type | Boundary1. amt | Sort Key | Order | Behavior                          |
| Case |                |                |          |       |                                   |
|------+----------------+----------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB            | ANY      | ANY   | start = 0                         |
|   2. | PRECEDING      | unsigned int   | NULL     | ASC   | start = 0                         |
|   3. |                |                |          | DESC  | scan backwards to row R2          |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   4. | PRECEDING      | unsigned int   | not NULL | DESC  | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|      |                |                |          |       | start = R2.idx + 1                |
|   5. | PRECEDING      | unsigned int   | not NULL | ASC   | scan backward until row R2        |
|      |                |                |          |       | such that R.sk - R2.sk > bnd1.amt |
|      |                |                |          |       | start = R2.idx + 1                |
|   6. | CURRENT ROW    |                | NULL     | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx + 1                |
|   7. | CURRENT ROW    |                | not NULL | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such R2.sk != R.sk                |
|      |                |                |          |       | start = R2.idx + 1                |
|   8. | FOLLOWING      | UNB            | ANY      | ANY   | Error                             |
|   9. | FOLLOWING      | unsigned int   | NULL     | DESC  | start = partition.size            |
|  10. |                |                |          | ASC   | scan forward until R2             |
|      |                |                |          |       | such that R2.sk is not null       |
|      |                |                |          |       | start = R2.idx                    |
|  11. | FOLLOWING      | unsigned int   | not NULL | DESC  | scan forward until row R2         |
|      |                |                |          |       | such that R.sk - R2.sk > amt      |
|      |                |                |          |       | start = R2.idx                    |
|  12. |                |                |          | ASC   | scan forward until row R2         |
|      |                |                |          |       | such that R2.sk - R.sk > amt      |
|------+----------------+----------------+----------+-------+-----------------------------------|
   */

  @Override
  public int computeStart(int rowIdx, PTFPartition p) throws HiveException {
    switch(start.getDirection()) {
    case PRECEDING:
      return computeStartPreceding(rowIdx, p);
    case CURRENT:
      return computeStartCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeStartFollowing(rowIdx, p);
    }
  }

  protected int computeStartPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    // Use Case 1.
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return 0;
    }
    Object sortKey = computeValueUseCache(rowIdx, p);

    if ( sortKey == null ) {
      // Use Case 3.
      if (nullsLast || expressionDef.getOrder() == Order.DESC) {
        while ( sortKey == null && rowIdx >= 0 ) {
          Pair<Integer, Object> stepResult = skipOrStepBack(rowIdx, p);
          rowIdx = stepResult.getLeft();
          sortKey = stepResult.getRight();
        }
        return rowIdx + 1;
      }
      else { // Use Case 2.
        if ( expressionDef.getOrder() == Order.ASC ) {
          return 0;
        }
      }
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 4.
    if ( expressionDef.getOrder() == Order.DESC ) {
      while (r >= 0 && !isDistanceGreater(rowVal, sortKey, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r + 1;
    }
    else { // Use Case 5.
      while (r >= 0 && !isDistanceGreater(sortKey, rowVal, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }

      return r + 1;
    }
  }

  protected int computeStartCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValueUseCache(rowIdx, p);

    // Use Case 6.
    if ( sortKey == null ) {
      while ( sortKey == null && rowIdx >= 0 ) {
        Pair<Integer, Object> stepResult = skipOrStepBack(rowIdx, p);
        rowIdx = stepResult.getLeft();
        sortKey = stepResult.getRight();
      }
      return rowIdx + 1;
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 7.
    while (r >= 0 && isEqual(rowVal, sortKey) ) {
      Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
      r = stepResult.getLeft();
      rowVal = stepResult.getRight();
    }
    return r + 1;
  }

  protected int computeStartFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    Object sortKey = computeValueUseCache(rowIdx, p);

    Object rowVal = sortKey;
    int r = rowIdx;

    if ( sortKey == null ) {
      // Use Case 9.
      if (nullsLast || expressionDef.getOrder() == Order.DESC) {
        return p.size();
      }
      else { // Use Case 10.
        while (r < p.size() && rowVal == null ) {
          Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
          r = stepResult.getLeft();
          rowVal = stepResult.getRight();
        }
        return r;
      }
    }

    // Use Case 11.
    if ( expressionDef.getOrder() == Order.DESC) {
      while (r < p.size() && !isDistanceGreater(sortKey, rowVal, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r;
    }
    else { // Use Case 12.
      while (r < p.size() && !isDistanceGreater(rowVal, sortKey, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r;
    }
  }

  /*
|  Use | Boundary2.type | Boundary2.amt | Sort Key | Order | Behavior                          |
| Case |                |               |          |       |                                   |
|------+----------------+---------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB           | ANY      | ANY   | Error                             |
|   2. | PRECEDING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|   3. |                |               |          | ASC   | end = 0                           |
|   4. | PRECEDING      | unsigned int  | not null | DESC  | scan backward until row R2        |
|      |                |               |          |       | such that R2.sk - R.sk > bnd.amt  |
|      |                |               |          |       | end = R2.idx + 1                  |
|   5. | PRECEDING      | unsigned int  | not null | ASC   | scan backward until row R2        |
|      |                |               |          |       | such that R.sk -  R2.sk > bnd.amt |
|      |                |               |          |       | end = R2.idx + 1                  |
|   6. | CURRENT ROW    |               | NULL     | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|   7. | CURRENT ROW    |               | not null | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk != R.sk           |
|      |                |               |          |       | end = R2.idx                      |
|   8. | FOLLOWING      | UNB           | ANY      | ANY   | end = partition.size()            |
|   9. | FOLLOWING      | unsigned int  | NULL     | DESC  | end = partition.size()            |
|  10. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk is not null       |
|      |                |               |          |       | end = R2.idx                      |
|  11. | FOLLOWING      | unsigned int  | not NULL | DESC  | scan forward until row R2         |
|      |                |               |          |       | such R.sk - R2.sk > bnd.amt       |
|      |                |               |          |       | end = R2.idx                      |
|  12. |                |               |          | ASC   | scan forward until row R2         |
|      |                |               |          |       | such R2.sk - R2.sk > bnd.amt      |
|      |                |               |          |       | end = R2.idx                      |
|------+----------------+---------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeEnd(int rowIdx, PTFPartition p) throws HiveException {
    switch(end.getDirection()) {
    case PRECEDING:
      return computeEndPreceding(rowIdx, p);
    case CURRENT:
      return computeEndCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeEndFollowing(rowIdx, p);
    }
  }

  protected int computeEndPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();
    // Use Case 1.
    // amt == UNBOUNDED, is caught during translation

    Object sortKey = computeValueUseCache(rowIdx, p);

    if ( sortKey == null ) {
      // Use Case 2.
      if (nullsLast || expressionDef.getOrder() == Order.DESC ) {
        return p.size();
      }
      else { // Use Case 3.
        return 0;
      }
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 4.
    if ( expressionDef.getOrder() == Order.DESC ) {
      while (r >= 0 && !isDistanceGreater(rowVal, sortKey, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r + 1;
    }
    else { // Use Case 5.
      while (r >= 0 && !isDistanceGreater(sortKey, rowVal, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r + 1;
    }
  }

  protected int computeEndCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValueUseCache(rowIdx, p);

    // Use Case 6.
    if ( sortKey == null ) {
      while ( sortKey == null && rowIdx < p.size() ) {
        Pair<Integer, Object> stepResult = skipOrStepForward(rowIdx, p);
        rowIdx = stepResult.getLeft();
        sortKey = stepResult.getRight();
      }
      return rowIdx;
    }

    Object rowVal = sortKey;
    int r = rowIdx;

    // Use Case 7.
    while (r < p.size() && isEqual(sortKey, rowVal) ) {
      Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
      r = stepResult.getLeft();
      rowVal = stepResult.getRight();
    }
    return r;
  }

  protected int computeEndFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();

    // Use Case 8.
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return p.size();
    }
    Object sortKey = computeValueUseCache(rowIdx, p);

    Object rowVal = sortKey;
    int r = rowIdx;

    if ( sortKey == null ) {
      // Use Case 9.
      if (nullsLast || expressionDef.getOrder() == Order.DESC) {
        return p.size();
      }
      else { // Use Case 10.
        while (r < p.size() && rowVal == null ) {
          Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
          r = stepResult.getLeft();
          rowVal = stepResult.getRight();
        }
        return r;
      }
    }

    // Use Case 11.
    if ( expressionDef.getOrder() == Order.DESC) {
      while (r < p.size() && !isDistanceGreater(sortKey, rowVal, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r;
    }
    else { // Use Case 12.
      while (r < p.size() && !isDistanceGreater(rowVal, sortKey, amt) ) {
        Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
        r = stepResult.getLeft();
        rowVal = stepResult.getRight();
      }
      return r;
    }
  }

  public Object computeValue(Object row) throws HiveException {
    Object o = expressionDef.getExprEvaluator().evaluate(row);
    return ObjectInspectorUtils.copyToStandardObject(o, expressionDef.getOI());
  }

  /**
   * Checks if the distance of v2 to v1 is greater than the given amt.
   * @return True if the value of v1 - v2 is greater than amt or either value is null.
   */
  public abstract boolean isDistanceGreater(Object v1, Object v2, int amt);

  /**
   * Checks if the values of v1 or v2 are the same.
   * @return True if both values are the same or both are nulls.
   */
  public abstract boolean isEqual(Object v1, Object v2);


  @SuppressWarnings("incomplete-switch")
  public static SingleValueBoundaryScanner getScanner(BoundaryDef start, BoundaryDef end,
      OrderDef orderDef, boolean nullsLast) throws HiveException {
    if (orderDef.getExpressions().size() != 1) {
      throw new HiveException("Internal error: initializing SingleValueBoundaryScanner with"
              + " multiple expression for sorting");
    }
    OrderExpressionDef exprDef = orderDef.getExpressions().get(0);
    PrimitiveObjectInspector pOI = (PrimitiveObjectInspector) exprDef.getOI();
    switch(pOI.getPrimitiveCategory()) {
    case BYTE:
    case INT:
    case LONG:
    case SHORT:
      return new LongValueBoundaryScanner(start, end, exprDef, nullsLast);
    case TIMESTAMP:
      return new TimestampValueBoundaryScanner(start, end, exprDef, nullsLast);
    case TIMESTAMPLOCALTZ:
      return new TimestampLocalTZValueBoundaryScanner(start, end, exprDef, nullsLast);
    case DOUBLE:
    case FLOAT:
      return new DoubleValueBoundaryScanner(start, end, exprDef, nullsLast);
    case DECIMAL:
      return new HiveDecimalValueBoundaryScanner(start, end, exprDef, nullsLast);
    case DATE:
      return new DateValueBoundaryScanner(start, end, exprDef, nullsLast);
    case STRING:
      return new StringValueBoundaryScanner(start, end, exprDef, nullsLast);
    }
    throw new HiveException(
        String.format("Internal Error: attempt to setup a Window for datatype %s",
            pOI.getPrimitiveCategory()));
  }
}

class LongValueBoundaryScanner extends SingleValueBoundaryScanner {
  public LongValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (l1 -l2) > amt;
    }

    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getLong(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      long l2 = PrimitiveObjectInspectorUtils.getLong(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return l1 == l2;
    }

    return v1 == null && v2 == null; // True if both are null
  }
}

class DoubleValueBoundaryScanner extends SingleValueBoundaryScanner {
  public DoubleValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return (d1 -d2) > amt;
    }

    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      double d1 = PrimitiveObjectInspectorUtils.getDouble(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      double d2 = PrimitiveObjectInspectorUtils.getDouble(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return d1 == d2;
    }

    return v1 == null && v2 == null; // True if both are null
  }
}

class HiveDecimalValueBoundaryScanner extends SingleValueBoundaryScanner {
  public HiveDecimalValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if ( d1 != null && d2 != null ) {
      return d1.subtract(d2).intValue() > amt;  // TODO: lossy conversion!
    }

    return d1 != null || d2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    HiveDecimal d1 = PrimitiveObjectInspectorUtils.getHiveDecimal(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    HiveDecimal d2 = PrimitiveObjectInspectorUtils.getHiveDecimal(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if ( d1 != null && d2 != null ) {
      return d1.equals(d2);
    }

    return d1 == null && d2 == null; // True if both are null
  }
}

class DateValueBoundaryScanner extends SingleValueBoundaryScanner {
  public DateValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    Date l1 = PrimitiveObjectInspectorUtils.getDate(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    Date l2 = PrimitiveObjectInspectorUtils.getDate(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    if (l1 != null && l2 != null) {
        return (double)(l1.toEpochMilli() - l2.toEpochMilli())/1000 > (long)amt * 24 * 3600; // Converts amt days to milliseconds
    }
    return l1 != l2; // True if only one date is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    Date l1 = PrimitiveObjectInspectorUtils.getDate(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    Date l2 = PrimitiveObjectInspectorUtils.getDate(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return (l1 == null && l2 == null) || (l1 != null && l1.equals(l2));
  }
}

class TimestampValueBoundaryScanner extends SingleValueBoundaryScanner {
  public TimestampValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end,expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
          (PrimitiveObjectInspector) expressionDef.getOI()).toEpochMilli();
      long l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
          (PrimitiveObjectInspector) expressionDef.getOI()).toEpochMilli();
      return (double)(l1-l2)/1000 > amt; // TODO: lossy conversion, distance is considered in seconds
    }
    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      Timestamp l1 = PrimitiveObjectInspectorUtils.getTimestamp(v1,
          (PrimitiveObjectInspector) expressionDef.getOI());
      Timestamp l2 = PrimitiveObjectInspectorUtils.getTimestamp(v2,
          (PrimitiveObjectInspector) expressionDef.getOI());
      return l1.equals(l2);
    }
    return v1 == null && v2 == null; // True if both are null
  }
}

class TimestampLocalTZValueBoundaryScanner extends SingleValueBoundaryScanner {
  public TimestampLocalTZValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    if (v1 != null && v2 != null) {
      long l1 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v1,
          (PrimitiveObjectInspector) expressionDef.getOI(), null).getEpochSecond();
      long l2 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v2,
          (PrimitiveObjectInspector) expressionDef.getOI(), null).getEpochSecond();
      return (l1 -l2) > amt; // TODO: lossy conversion, distance is considered seconds similar to timestamp
    }
    return v1 != null || v2 != null; // True if only one value is null
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    if (v1 != null && v2 != null) {
      TimestampTZ l1 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v1,
          (PrimitiveObjectInspector) expressionDef.getOI(), null);
      TimestampTZ l2 = PrimitiveObjectInspectorUtils.getTimestampLocalTZ(v2,
          (PrimitiveObjectInspector) expressionDef.getOI(), null);
      return l1.equals(l2);
    }
    return v1 == null && v2 == null; // True if both are null
  }
}

class StringValueBoundaryScanner extends SingleValueBoundaryScanner {
  public StringValueBoundaryScanner(BoundaryDef start, BoundaryDef end,
      OrderExpressionDef expressionDef, boolean nullsLast) {
    super(start, end, expressionDef, nullsLast);
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    String s1 = PrimitiveObjectInspectorUtils.getString(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    String s2 = PrimitiveObjectInspectorUtils.getString(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return s1 != null && s2 != null && s1.compareTo(s2) > amt;
  }

  @Override
  public boolean isEqual(Object v1, Object v2) {
    String s1 = PrimitiveObjectInspectorUtils.getString(v1,
        (PrimitiveObjectInspector) expressionDef.getOI());
    String s2 = PrimitiveObjectInspectorUtils.getString(v2,
        (PrimitiveObjectInspector) expressionDef.getOI());
    return (s1 == null && s2 == null) || (s1 != null && s1.equals(s2));
  }
}

/*
 */
 class MultiValueBoundaryScanner extends ValueBoundaryScanner {
  OrderDef orderDef;

  public MultiValueBoundaryScanner(BoundaryDef start, BoundaryDef end, OrderDef orderDef,
      boolean nullsLast) {
    super(start, end, nullsLast);
    this.orderDef = orderDef;
  }

  /*
|------+----------------+----------------+----------+-------+-----------------------------------|
| Use  | Boundary1.type | Boundary1. amt | Sort Key | Order | Behavior                          |
| Case |                |                |          |       |                                   |
|------+----------------+----------------+----------+-------+-----------------------------------|
|   1. | PRECEDING      | UNB            | ANY      | ANY   | start = 0                         |
|   2. | CURRENT ROW    |                | ANY      | ANY   | scan backwards until row R2       |
|      |                |                |          |       | such R2.sk != R.sk                |
|      |                |                |          |       | start = R2.idx + 1                |
|------+----------------+----------------+----------+-------+-----------------------------------|
   */
  @Override
  public int computeStart(int rowIdx, PTFPartition p) throws HiveException {
    switch(start.getDirection()) {
    case PRECEDING:
      return computeStartPreceding(rowIdx, p);
    case CURRENT:
      return computeStartCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        throw new HiveException(
                "FOLLOWING not allowed for starting RANGE with multiple expressions in ORDER BY");
    }
  }

  protected int computeStartPreceding(int rowIdx, PTFPartition p) throws HiveException {
    int amt = start.getAmt();
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return 0;
    }
    throw new HiveException(
            "PRECEDING needs UNBOUNDED for RANGE with multiple expressions in ORDER BY");
  }

  protected int computeStartCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValueUseCache(rowIdx, p);
    Object rowVal = sortKey;
    int r = rowIdx;

    while (r >= 0 && isEqual(rowVal, sortKey) ) {
      Pair<Integer, Object> stepResult = skipOrStepBack(r, p);
      r = stepResult.getLeft();
      rowVal = stepResult.getRight();
    }
    return r + 1;
  }

  /*
|------+----------------+---------------+----------+-------+-----------------------------------|
| Use  | Boundary2.type | Boundary2.amt | Sort Key | Order | Behavior                          |
| Case |                |               |          |       |                                   |
|------+----------------+---------------+----------+-------+-----------------------------------|
|   1. | CURRENT ROW    |               | ANY      | ANY   | scan forward until row R2         |
|      |                |               |          |       | such that R2.sk != R.sk           |
|      |                |               |          |       | end = R2.idx                      |
|   2. | FOLLOWING      | UNB           | ANY      | ANY   | end = partition.size()            |
|------+----------------+---------------+----------+-------+-----------------------------------|
   */

  @Override
  public int computeEnd(int rowIdx, PTFPartition p) throws HiveException {
    switch(end.getDirection()) {
    case PRECEDING:
      throw new HiveException(
              "PRECEDING not allowed for finishing RANGE with multiple expressions in ORDER BY");
    case CURRENT:
      return computeEndCurrentRow(rowIdx, p);
    case FOLLOWING:
      default:
        return computeEndFollowing(rowIdx, p);
    }
  }

  protected int computeEndCurrentRow(int rowIdx, PTFPartition p) throws HiveException {
    Object sortKey = computeValueUseCache(rowIdx, p);
    Object rowVal = sortKey;
    int r = rowIdx;

    while (r < p.size() && isEqual(sortKey, rowVal) ) {
      Pair<Integer, Object> stepResult = skipOrStepForward(r, p);
      r = stepResult.getLeft();
      rowVal = stepResult.getRight();
    }
    return r;
  }

  protected int computeEndFollowing(int rowIdx, PTFPartition p) throws HiveException {
    int amt = end.getAmt();
    if ( amt == BoundarySpec.UNBOUNDED_AMOUNT ) {
      return p.size();
    }
    throw new HiveException(
            "FOLLOWING needs UNBOUNDED for RANGE with multiple expressions in ORDER BY");
  }

  @Override
  public Object computeValue(Object row) throws HiveException {
    Object[] objs = new Object[orderDef.getExpressions().size()];
    for (int i = 0; i < objs.length; i++) {
      Object o = orderDef.getExpressions().get(i).getExprEvaluator().evaluate(row);
      objs[i] = ObjectInspectorUtils.copyToStandardObject(o, orderDef.getExpressions().get(i).getOI());
    }
    return objs;
  }

  @Override
  public boolean isEqual(Object val1, Object val2) {
    if (val1 == null || val2 == null) {
      return (val1 == null && val2 == null);
    }
    Object[] v1 = (Object[]) val1;
    Object[] v2 = (Object[]) val2;

    assert v1.length == v2.length;
    for (int i = 0; i < v1.length; i++) {
      if (v1[i] == null && v2[i] == null) {
        continue;
      }
      if (v1[i] == null || v2[i] == null) {
        return false;
      }
      if (ObjectInspectorUtils.compare(
              v1[i], orderDef.getExpressions().get(i).getOI(),
              v2[i], orderDef.getExpressions().get(i).getOI()) != 0) {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean isDistanceGreater(Object v1, Object v2, int amt) {
    throw new UnsupportedOperationException("Only unbounded ranges supported");
  }
}

