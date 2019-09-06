package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

public class ScalarNullOrCol extends ColOrCol {
  private static final long serialVersionUID = 1L;
  protected final int colNum;

  public ScalarNullOrCol(ConstantVectorExpression expression, int colNum, int outputColumnNum) {
    super(colNum, -1, outputColumnNum);
    this.colNum = colNum;
  }

  public ScalarNullOrCol() {
    super();
    colNum = -1;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (childExpressions != null) {
      super.evaluateChildren(batch);
    }

    LongColumnVector inputColVector = (LongColumnVector) batch.cols[colNum];

    super.doEvaluate(batch, new LongColumnVector(inputColVector.vector.length).fillWithNulls(),
        inputColVector);
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
