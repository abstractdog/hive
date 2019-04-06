/*
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
package org.apache.hive.benchmark.vectorization;

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.CastLongToDecimal;
import org.apache.hive.benchmark.vectorization.expressions.old.CastLongToDecimalOld;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

@State(Scope.Benchmark)
public class VectorizedExpressionBench {

  public static class CastLongToDecimalBench extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DecimalColumnVector(38, 18), 1, new LongColumnVector());
      expression = new CastLongToDecimal(0, 1);
    }
  }

  public static class CastLongToDecimalBenchOld extends AbstractExpression {
    @Override
    public void setup() {
      rowBatch = buildRowBatch(new DecimalColumnVector(38, 18), 1, new LongColumnVector());
      expression = new CastLongToDecimalOld(0, 1);
    }
  }

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder().include(".*" + VectorizedExpressionBench.class.getSimpleName() + ".*").build();
    new Runner(opt).run();
  }
}
