
package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Base class for vector expressions which don't want to copy the plain old
 * loop-in-loop-and-call-business-function logic
 *
 */
public abstract class BaseVectorExpression<O, I> extends VectorExpression {
  private static final long serialVersionUID = 1L;
  protected int inputColumn;

  public BaseVectorExpression(int inputColumn, int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumn = inputColumn;
  }

  public BaseVectorExpression(int outputColumnNum) {
    super(outputColumnNum);
    this.inputColumn = -1;
  }

  public BaseVectorExpression() {
    super();
    // Dummy final assignments.
    inputColumn = -1;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {

    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[inputColumn];
    int[] sel = batch.selected;
    int n = batch.size;
    ColumnVector outputColVector = batch.cols[outputColumnNum];
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;

    beforeLoop((O) outputColVector, (I) inputColVector);

    if (n == 0) {
      // Nothing to do
      return;
    }

    // We do not need to do a column reset since we are carefully changing the output.
    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        // Set isNull before call in case it changes it mind.
        outputIsNull[0] = false;
        func((O) outputColVector, (I) inputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            // Set isNull before call in case it changes it mind.
            outputIsNull[i] = false;
            func((O) outputColVector, (I) inputColVector, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            func((O) outputColVector, (I) inputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          func((O) outputColVector, (I) inputColVector, i);
        }
      }
    } else /* there are nulls in the inputColVector */ {

      // Carefully handle NULLs...
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputColVector.isNull[i] = inputColVector.isNull[i];
          if (!inputColVector.isNull[i]) {
            func((O) outputColVector, (I) inputColVector, i);
          }
        }
      } else {
        System.arraycopy(inputColVector.isNull, 0, outputColVector.isNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            func((O) outputColVector, (I) inputColVector, i);
          }
        }
      }
    }
  }

  protected void beforeLoop(O outputColVector, I inputColVector) {
  }

  abstract protected void func(O outputColVector, I inputColVector, int batchIndex);
}
