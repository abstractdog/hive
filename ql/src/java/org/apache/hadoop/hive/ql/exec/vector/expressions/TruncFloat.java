package org.apache.hadoop.hive.ql.exec.vector.expressions;

import java.math.BigDecimal;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Descriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

/**
 * Vectorized implementation of trunc(number, scale) function
 */
public class TruncFloat extends VectorExpression {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;
  protected int colNum;
  protected int scale;
  protected BigDecimal pow = BigDecimal.valueOf(Math.pow(10, Math.abs(scale)));

  public TruncFloat() {
    super();
    colNum = -1;
  }

  public TruncFloat(int colNum, int scale, int outputColumnNum) {
    super(outputColumnNum);
    this.colNum = colNum;
    this.scale = scale;

    if (scale >= 0) {
      pow = BigDecimal.valueOf(Math.pow(10, scale));
    }
  }

  @Override
  public String vectorExpressionParameters() {
    return "col " + colNum + ", scale " + scale;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      this.evaluateChildren(batch);
    }

    ColumnVector inputColVector = batch.cols[colNum];
    ColumnVector outputColVector = batch.cols[outputColumnNum];

    int[] sel = batch.selected;
    boolean[] inputIsNull = inputColVector.isNull;
    boolean[] outputIsNull = outputColVector.isNull;
    int n = batch.size;

    if (n == 0) {
      return;
    }

    outputColVector.isRepeating = false;

    if (inputColVector.isRepeating) {
      if (inputColVector.noNulls || !inputIsNull[0]) {
        outputIsNull[0] = false;
        trunc(inputColVector, outputColVector, 0);
      } else {
        outputIsNull[0] = true;
        outputColVector.noNulls = false;
      }
      outputColVector.isRepeating = true;
      return;
    }

    if (inputColVector.noNulls) {
      if (batch.selectedInUse) {
        if (!outputColVector.noNulls) {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            outputIsNull[i] = false;
            trunc(inputColVector, outputColVector, i);
          }
        } else {
          for (int j = 0; j != n; j++) {
            final int i = sel[j];
            trunc(inputColVector, outputColVector, i);
          }
        }
      } else {
        if (!outputColVector.noNulls) {
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for (int i = 0; i != n; i++) {
          trunc(inputColVector, outputColVector, i);
        }
      }
    } else {
      outputColVector.noNulls = false;

      if (batch.selectedInUse) {
        for (int j = 0; j != n; j++) {
          int i = sel[j];
          outputIsNull[i] = inputIsNull[i];
          trunc(inputColVector, outputColVector, i);
        }
      } else {
        System.arraycopy(inputIsNull, 0, outputIsNull, 0, n);
        for (int i = 0; i != n; i++) {
          if (!inputColVector.isNull[i]) {
            trunc(inputColVector, outputColVector, i);
          }
        }
      }
    }
  }

  protected void trunc(ColumnVector inputColVector, ColumnVector outputColVector, int i) {
    BigDecimal input = BigDecimal.valueOf(((DoubleColumnVector) inputColVector).vector[i]);
    double output = DoubleColumnVector.NULL_VALUE;

    output = trunc(input).doubleValue();

    ((DoubleColumnVector)outputColVector).vector[i] = output;
  }

  protected BigDecimal trunc(BigDecimal input) {
    BigDecimal output = new BigDecimal(0);
    BigDecimal pow = BigDecimal.valueOf(Math.pow(10, Math.abs(scale)));
    if (scale >= 0) {
      pow = BigDecimal.valueOf(Math.pow(10, scale));
      if (scale != 0) {
        long longValue = input.multiply(pow).longValue();
        output = BigDecimal.valueOf(longValue).divide(pow);
      } else {
        output = BigDecimal.valueOf(input.longValue());
      }
    } else {
      long longValue2 = input.divide(pow).longValue();
      output = BigDecimal.valueOf(longValue2).multiply(pow);
    }
    return output;
  }

  @Override
  public Descriptor getDescriptor() {
    VectorExpressionDescriptor.Builder b = new VectorExpressionDescriptor.Builder();
    b.setMode(VectorExpressionDescriptor.Mode.PROJECTION).setNumArguments(2)
        .setArgumentTypes(VectorExpressionDescriptor.ArgumentType.FLOAT_FAMILY,
            VectorExpressionDescriptor.ArgumentType.INT_FAMILY)
        .setInputExpressionTypes(VectorExpressionDescriptor.InputExpressionType.COLUMN,
            VectorExpressionDescriptor.InputExpressionType.SCALAR);
    return b.build();
  }
}
