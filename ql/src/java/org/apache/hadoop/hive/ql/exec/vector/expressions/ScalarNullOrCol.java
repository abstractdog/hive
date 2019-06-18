package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ScalarNullOrCol extends ScalarOrCol {
  private static final long serialVersionUID = 1L;

  public ScalarNullOrCol(long scalarVal, int colNum, int outputColumnNum) {
    super(0, colNum, outputColumnNum);
  }

  public ScalarNullOrCol() {
    super();
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    LongColumnVector outV = (LongColumnVector) batch.cols[outputColumnNum];
    long[] outputVector = outV.vector;
    boolean[] outputIsNull = outV.isNull;

    // null or "anything" is null
    outV.isRepeating = true;
    outputVector[0] = LongColumnVector.NULL_VALUE;
    outputIsNull[0] = true;
  }

  @Override
  public String vectorExpressionParameters() {
    return getColumnParamString(0, colNum) + ", " + getLongValueParamString(1, 0);
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder())
        .setMode(
            VectorExpressionDescriptor.Mode.PROJECTION)
        .setNumArguments(2)
        .setArgumentTypes(
            VectorExpressionDescriptor.ArgumentType.getType("long"),
            VectorExpressionDescriptor.ArgumentType.getType("long"))
        .setInputExpressionTypes(
            VectorExpressionDescriptor.InputExpressionType.NULLSCALAR,
            VectorExpressionDescriptor.InputExpressionType.COLUMN).build();
  }
}
