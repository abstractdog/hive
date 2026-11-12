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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.Mode;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorAggregationDesc;
import org.apache.hadoop.hive.ql.exec.vector.VectorGroupByOperator;
import org.apache.hadoop.hive.ql.exec.vector.VectorizationContext;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorUDAFCountStar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.gen.VectorUDAFSumLong;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.Vectorizer.VectorizerCannotVectorizeException;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.VectorGroupByDesc.ProcessingMode;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.junit.Before;
import org.junit.Test;

public class TestVectorizer {

  static VectorizationContext vContext = null;

  @Before
  public void setUp() {
    List<String> columns = new ArrayList<String>();
    columns.add("col0");
    columns.add("col1");
    columns.add("col2");
    columns.add("col3");

    //Generate vectorized expression
    vContext = new VectorizationContext("name", columns);
  }

  @Description(name = "fake", value = "FAKE")
  static class FakeGenericUDF extends GenericUDF {
    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
      return null;
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
      return null;
    }

    @Override
    public String getDisplayString(String[] children) {
      return "fake";
    }
  }

  @Test
  public void testAggregateOnUDF() throws HiveException, VectorizerCannotVectorizeException {
    ExprNodeColumnDesc colExprA = new ExprNodeColumnDesc(Integer.class, "col1", "T", false);
    ExprNodeColumnDesc colExprB = new ExprNodeColumnDesc(Integer.class, "col2", "T", false);

    List<ExprNodeDesc> children = new ArrayList<ExprNodeDesc>();
    children.add(colExprA);
    ExprNodeGenericFuncDesc exprNodeDesc = new ExprNodeGenericFuncDesc(TypeInfoFactory.intTypeInfo, new GenericUDFAbs(), children);

    ArrayList<ExprNodeDesc> params = new ArrayList<ExprNodeDesc>();
    params.add(exprNodeDesc);

    List<ObjectInspector> paramOIs = new ArrayList<ObjectInspector>();
    paramOIs.add(exprNodeDesc.getWritableObjectInspector());

    AggregationDesc aggDesc = new AggregationDesc("sum",
        FunctionRegistry.getGenericUDAFEvaluator("sum", paramOIs, false, false),
        params,
        false,
        GenericUDAFEvaluator.Mode.PARTIAL1);

    ArrayList<String> outputColumnNames = new ArrayList<String>();
    outputColumnNames.add("_col0");

    GroupByDesc desc = new GroupByDesc();
    VectorGroupByDesc vectorDesc = new VectorGroupByDesc();
    vectorDesc.setProcessingMode(ProcessingMode.HASH);
    vectorDesc.setVecAggrDescs(
        new VectorAggregationDesc[] {
            new VectorAggregationDesc.VectorAggregationDescBuilder()
                .aggregationName(aggDesc.getGenericUDAFName())
                .evaluator(new GenericUDAFSum.GenericUDAFSumLong())
                .udafEvaluatorMode(aggDesc.getMode())
                .inputTypeInfo(TypeInfoFactory.longTypeInfo)
                .inputColVectorType(ColumnVector.Type.LONG)
                .outputTypeInfo(TypeInfoFactory.longTypeInfo)
                .outputColVectorType(ColumnVector.Type.LONG)
                .vectorAggregationClass(VectorUDAFCountStar.class)
                .build()});
    desc.setOutputColumnNames(outputColumnNames);
    ArrayList<AggregationDesc> aggDescList = new ArrayList<AggregationDesc>();
    aggDescList.add(aggDesc);

    desc.setAggregators(aggDescList);

    ArrayList<ExprNodeDesc> grpByKeys = new ArrayList<ExprNodeDesc>();
    grpByKeys.add(colExprB);
    desc.setKeys(grpByKeys);

    Operator<? extends OperatorDesc> gbyOp = OperatorFactory.get(new CompilationOpContext(), desc);

    desc.setMode(GroupByDesc.Mode.HASH);

    VectorizationContext ctx = new VectorizationContext("name", Arrays.asList(new String[] {"col1", "col2"}));

    Vectorizer v = new Vectorizer();
    v.testSetCurrentBaseWork(new MapWork());
    VectorGroupByOperator vectorOp =
        (VectorGroupByOperator) Vectorizer.vectorizeGroupByOperator(gbyOp, ctx, vectorDesc);

    Assert.assertEquals(VectorUDAFSumLong.class, vectorDesc.getVecAggrDescs()[0].getVecAggrClass());
  }

  @Test
  public void testValidateNestedExpressions() {
    ExprNodeColumnDesc col1Expr = new ExprNodeColumnDesc(Integer.class, "col1", "table", false);
    ExprNodeConstantDesc constDesc = new ExprNodeConstantDesc(Integer.valueOf(10));

    GenericUDFOPGreaterThan udf = new GenericUDFOPGreaterThan();
    ExprNodeGenericFuncDesc greaterExprDesc = new ExprNodeGenericFuncDesc();
    greaterExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    greaterExprDesc.setGenericUDF(udf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(col1Expr);
    children1.add(constDesc);
    greaterExprDesc.setChildren(children1);

    FakeGenericUDF udf2 = new FakeGenericUDF();
    ExprNodeGenericFuncDesc nonSupportedExpr = new ExprNodeGenericFuncDesc();
    nonSupportedExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    nonSupportedExpr.setGenericUDF(udf2);

    GenericUDFOPAnd andUdf = new GenericUDFOPAnd();
    ExprNodeGenericFuncDesc andExprDesc = new ExprNodeGenericFuncDesc();
    andExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    andExprDesc.setGenericUDF(andUdf);
    List<ExprNodeDesc> children3 = new ArrayList<ExprNodeDesc>(2);
    children3.add(greaterExprDesc);
    children3.add(nonSupportedExpr);
    andExprDesc.setChildren(children3);

    Vectorizer v = new Vectorizer();
    v.testSetCurrentBaseWork(new MapWork());
    Assert.assertTrue(v.validateExprNodeDesc(andExprDesc, "test", VectorExpressionDescriptor.Mode.FILTER, false));
  }

  /**
  * prepareAbstractMapJoin prepares a join operator descriptor, used as helper by SMB and Map join tests.
  */
  private void prepareAbstractMapJoin(AbstractMapJoinOperator<? extends MapJoinDesc> map, MapJoinDesc mjdesc) {
      mjdesc.setPosBigTable(0);
      List<ExprNodeDesc> expr = new ArrayList<ExprNodeDesc>();
      expr.add(new ExprNodeColumnDesc(Integer.class, "col1", "T", false));
      Map<Byte, List<ExprNodeDesc>> keyMap = new HashMap<Byte, List<ExprNodeDesc>>();
      keyMap.put((byte)0, expr);
      List<ExprNodeDesc> smallTableExpr = new ArrayList<ExprNodeDesc>();
      smallTableExpr.add(new ExprNodeColumnDesc(Integer.class, "col2", "T1", false));
      keyMap.put((byte)1, smallTableExpr);
      mjdesc.setKeys(keyMap);
      mjdesc.setExprs(keyMap);
      Byte[] order = new Byte[] {(byte) 0, (byte) 1};
      mjdesc.setTagOrder(order);

      //Set filter expression
      GenericUDFOPEqual udf = new GenericUDFOPEqual();
      ExprNodeGenericFuncDesc equalExprDesc = new ExprNodeGenericFuncDesc();
      equalExprDesc.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
      equalExprDesc.setGenericUDF(udf);
      List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
      children1.add(new ExprNodeColumnDesc(Integer.class, "col2", "T1", false));
      children1.add(new ExprNodeColumnDesc(Integer.class, "col3", "T2", false));
      equalExprDesc.setChildren(children1);
      List<ExprNodeDesc> filterExpr = new ArrayList<ExprNodeDesc>();
      filterExpr.add(equalExprDesc);
      Map<Byte, List<ExprNodeDesc>> filterMap = new HashMap<Byte, List<ExprNodeDesc>>();
      filterMap.put((byte) 0, expr);
      mjdesc.setFilters(filterMap);
 }

  /**
  * testValidateMapJoinOperator validates that the Map join operator can be vectorized.
  */
  @Test
  public void testValidateMapJoinOperator() {
    MapJoinOperator map = new MapJoinOperator(new CompilationOpContext());
    MapJoinDesc mjdesc = new MapJoinDesc();

    prepareAbstractMapJoin(map, mjdesc);
    map.setConf(mjdesc);

    Vectorizer vectorizer = new Vectorizer();
    vectorizer.testSetCurrentBaseWork(new MapWork());
    // UNDONE
    // Assert.assertTrue(vectorizer.validateMapWorkOperator(map, null, false));
  }


  /**
  * testValidateSMBJoinOperator validates that the SMB join operator can be vectorized.
  */
  @Test
  public void testValidateSMBJoinOperator() {
      SMBMapJoinOperator map = new SMBMapJoinOperator(new CompilationOpContext());
      SMBJoinDesc mjdesc = new SMBJoinDesc();

      prepareAbstractMapJoin(map, mjdesc);
      map.setConf(mjdesc);

      Vectorizer vectorizer = new Vectorizer();
      vectorizer.testSetCurrentBaseWork(new MapWork());
      // UNDONE
      // Assert.assertTrue(vectorizer.validateMapWorkOperator(map, null, false));
  }

  @Test
  public void testExprNodeDynamicValue() {
    ExprNodeDesc exprNode = new ExprNodeDynamicValueDesc(new DynamicValue("id1", TypeInfoFactory.stringTypeInfo));
    Vectorizer v = new Vectorizer();
    Assert.assertTrue(v.validateExprNodeDesc(exprNode, "Test", Mode.FILTER, false));
    Assert.assertTrue(v.validateExprNodeDesc(exprNode, "Test", Mode.PROJECTION, false));
  }

  @Test
  public void testExprNodeBetweenWithDynamicValue() {
    ExprNodeDesc notBetween = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, Boolean.FALSE);
    ExprNodeColumnDesc colExpr = new ExprNodeColumnDesc(String.class, "col1", "table", false);
    ExprNodeDesc minExpr = new ExprNodeDynamicValueDesc(new DynamicValue("id1", TypeInfoFactory.stringTypeInfo));
    ExprNodeDesc maxExpr = new ExprNodeDynamicValueDesc(new DynamicValue("id2", TypeInfoFactory.stringTypeInfo));

    ExprNodeGenericFuncDesc betweenExpr = new ExprNodeGenericFuncDesc();
    GenericUDF betweenUdf = new GenericUDFBetween();
    betweenExpr.setTypeInfo(TypeInfoFactory.booleanTypeInfo);
    betweenExpr.setGenericUDF(betweenUdf);
    List<ExprNodeDesc> children1 = new ArrayList<ExprNodeDesc>(2);
    children1.add(notBetween);
    children1.add(colExpr);
    children1.add(minExpr);
    children1.add(maxExpr);
    betweenExpr.setChildren(children1);

    Vectorizer v = new Vectorizer();
    v.testSetCurrentBaseWork(new MapWork());
    boolean valid = v.validateExprNodeDesc(betweenExpr, "Test", Mode.FILTER, false);
    Assert.assertTrue(valid);
  }

  @Test
  public void testGroupingSetParents() {
    // a grouping set is the mask of the keys it leaves out; its parent is the set with the fewest keys containing it
    // ROLLUP(a, b, c): (a, b, c), (a, b), (a), ()
    Assert.assertArrayEquals(new int[] {-1, 0, 1, 2}, Vectorizer.groupingSetParents(Arrays.asList(0L, 1L, 3L, 7L)));
    // CUBE(a, b): (a, b), (a), (b), (); () has two parents of one key, the first is taken
    Assert.assertArrayEquals(new int[] {-1, 0, 0, 1}, Vectorizer.groupingSetParents(Arrays.asList(0L, 1L, 2L, 3L)));
    // GROUPING SETS ((a, b), (a, c), (a), ()) over a, b, c: two sets no other contains
    Assert.assertArrayEquals(new int[] {-1, -1, 0, 2}, Vectorizer.groupingSetParents(Arrays.asList(1L, 2L, 3L, 7L)));
  }

  /**
   * Counts initializations: validating a partition's schema costs the Vectorizer one.
   */
  public static class CountingParquetHiveSerDe extends ParquetHiveSerDe {
    static final AtomicInteger INITIALIZE_COUNT = new AtomicInteger();

    @Override
    public void initialize(Configuration configuration, Properties tableProperties, Properties partitionProperties)
        throws SerDeException {
      INITIALIZE_COUNT.incrementAndGet();
      super.initialize(configuration, tableProperties, partitionProperties);
    }
  }

  private static final String ALIAS = "t";

  @Test
  public void testPartitionsSharingASchemaAreValidatedOnce() throws Exception {
    HiveConf conf = new HiveConfForTest(getClass());
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED, true);
    conf.setBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_USE_VECTORIZED_INPUT_FILE_FORMAT, true);
    // LLAP I/O rejects partitions that need type conversion, like the evolved one below
    conf.set(HiveConf.ConfVars.LLAP_IO_ENABLED.varname, "false");
    SessionState sessionState = SessionState.start(conf);
    try {
      Properties tableProps = new Properties();
      tableProps.setProperty(serdeConstants.SERIALIZATION_LIB, CountingParquetHiveSerDe.class.getName());
      tableProps.setProperty(serdeConstants.LIST_COLUMNS, "c1,c2");
      tableProps.setProperty(serdeConstants.LIST_COLUMN_TYPES, "int:string");
      tableProps.setProperty(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "p");
      TableDesc tableDesc = new TableDesc(MapredParquetInputFormat.class, MapredParquetOutputFormat.class, tableProps);

      MapWork mapWork = new MapWork();
      Map<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<>();
      aliasToWork.put(ALIAS, createTableScan());
      mapWork.setAliasToWork(aliasToWork);
      // three partitions that differ only in location, basic stats and DDL time, then one whose
      // first column evolved from int to bigint
      List<PartitionDesc> sameSchema = new ArrayList<>();
      for (int i = 0; i < 3; i++) {
        sameSchema.add(addPartition(mapWork, tableDesc, i, "int:string"));
      }
      PartitionDesc evolved = addPartition(mapWork, tableDesc, 3, "bigint:string");

      MapredWork mapredWork = new MapredWork();
      mapredWork.setMapWork(mapWork);
      List<Task<?>> rootTasks = Collections.singletonList(TaskFactory.get(mapredWork));
      CountingParquetHiveSerDe.INITIALIZE_COUNT.set(0);
      new Vectorizer().resolve(new PhysicalContext(conf, null, new Context(conf), rootTasks, null));

      Assert.assertTrue(mapWork.getVectorizationEnabled());
      Assert.assertEquals("SerDe initializations: one per distinct schema",
          2, CountingParquetHiveSerDe.INITIALIZE_COUNT.get());
      VectorPartitionDesc shared = sameSchema.get(0).getVectorPartitionDesc();
      Assert.assertNotNull(shared);
      for (PartitionDesc partDesc : sameSchema) {
        Assert.assertSame(shared, partDesc.getVectorPartitionDesc());
      }
      Assert.assertNotNull(evolved.getVectorPartitionDesc());
      Assert.assertNotSame(shared, evolved.getVectorPartitionDesc());
      Assert.assertEquals(2, mapWork.getVectorPartitionDescList().size());
    } finally {
      sessionState.close();
    }
  }

  private static TableScanOperator createTableScan() {
    TableScanOperator ts =
        (TableScanOperator) OperatorFactory.get(new CompilationOpContext(), new TableScanDesc(ALIAS, null));
    ts.setSchema(new RowSchema(Arrays.asList(
        new ColumnInfo("c1", TypeInfoFactory.intTypeInfo, ALIAS, false),
        new ColumnInfo("c2", TypeInfoFactory.stringTypeInfo, ALIAS, false),
        new ColumnInfo("p", TypeInfoFactory.stringTypeInfo, ALIAS, true))));
    ts.setNeededColumns(Arrays.asList("c1", "c2"));
    ts.setNeededColumnIDs(Arrays.asList(0, 1));
    return ts;
  }

  private static PartitionDesc addPartition(MapWork mapWork, TableDesc tableDesc, int i, String columnTypes) {
    Path path = new Path("file:///warehouse/t/p=" + i);
    Properties props = new Properties();
    props.putAll(tableDesc.getProperties());
    props.setProperty(serdeConstants.LIST_COLUMN_TYPES, columnTypes);
    props.setProperty(hive_metastoreConstants.META_TABLE_LOCATION, path.toString());
    props.setProperty(hive_metastoreConstants.DDL_TIME, String.valueOf(1_700_000_000L + i));
    props.setProperty(StatsSetupConst.NUM_FILES, String.valueOf(1 + i));
    props.setProperty(StatsSetupConst.ROW_COUNT, String.valueOf(1000 + i));
    props.setProperty(StatsSetupConst.TOTAL_SIZE, String.valueOf(50_000 + i));
    Map<String, String> partSpec = new LinkedHashMap<>();
    partSpec.put("p", String.valueOf(i));
    PartitionDesc partDesc = new PartitionDesc(tableDesc, partSpec);
    partDesc.setInputFileFormatClass(MapredParquetInputFormat.class);
    partDesc.setProperties(props);
    mapWork.addPathToAlias(path, ALIAS);
    mapWork.addPathToPartitionInfo(path, partDesc);
    return partDesc;
  }
}
