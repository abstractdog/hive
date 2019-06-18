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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * This class performs OR expression on a scalar input and an input column and stores
 * the boolean output in a separate output column.
 */
public class ScalarOrCol extends VectorExpression {

  private static final long serialVersionUID = 1L;

  protected final int colNum;
  private final long scalarVal;

  public ScalarOrCol(long scalarVal, int colNum, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.scalarVal = scalarVal;
  }

  public ScalarOrCol() {
    super();

    // Dummy final assignments.
    colNum = -1;
    scalarVal = LongColumnVector.NULL_VALUE;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];

    int[] sel = batch.selected;
    int n = batch.size;
    long[] vector = inputColVector.vector;

    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outV.vector;
    if (n <= 0) {
      // Nothing to do
      return;
    }

    boolean[] outputIsNull = outV.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    outV.isRepeating = false;

    if (inputColVector.noNulls) {
      if (inputColVector.isRepeating) {

        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        outV.isRepeating = true;
        outputIsNull[0] = false;
        outputVector[0] = vector[0] | scalarVal;
      } else if (!inputColVector.isRepeating) {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputIsNull[i] = false;
            outputVector[i] = vector[i] | scalarVal;
          }
        } else {
          Arrays.fill(outputIsNull, 0, n, false);
          for (int i = 0; i != n; i++) {
            outputVector[i] = vector[i] | scalarVal;
          }
        }
      }
      return;
    }

    // Carefully handle NULLs...

    /*
     * For better performance on LONG/DOUBLE we don't want the conditional
     * statements inside the for loop.
     */
    outV.noNulls = false;

    if (!inputColVector.noNulls) {
      // only input 1 side has nulls
      if (inputColVector.isRepeating) {
        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector[0] | scalarVal;
        outputIsNull[0] = inputColVector.isNull[0] && (scalarVal == 0);
      } else if (!inputColVector.isRepeating) {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector[i] | scalarVal;
            outputIsNull[i] = inputColVector.isNull[i] && (scalarVal == 0);
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = vector[i] | scalarVal;
            outputIsNull[i] = inputColVector.isNull[i] && (scalarVal == 0);
          }
        }
      }
    } else /* !inputColVector1.noNulls && nullScalar */ {
      // either input 1 or input 2 may have nulls
      if (inputColVector.isRepeating) {
        // All must be selected otherwise size would be zero
        // Repeating property will not change.
        outV.isRepeating = true;
        outputVector[0] = vector[0] | scalarVal;
        outputIsNull[0] = inputColVector.isNull[0] && (scalarVal == 0);
      } else if (!inputColVector.isRepeating) {
        if (batch.selectedInUse) {
          for (int j = 0; j != n; j++) {
            int i = sel[j];
            outputVector[i] = vector[i] | scalarVal;
            outputIsNull[i] = inputColVector.isNull[i] && (scalarVal == 0);
          }
        } else {
          for (int i = 0; i != n; i++) {
            outputVector[i] = vector[i] | scalarVal;
            outputIsNull[i] = inputColVector.isNull[i] && (scalarVal == 0);
          }
        }
      }
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum) + ", " + getLongValueParamString(1, scalarVal);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).setMode(VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.SCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN)
        .build();
  }
}
