package org.apache.pinot.core.operator.transform.function;

import java.util.List;
import java.util.Map;
import org.apache.pinot.common.function.TransformFunctionType;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;


/**
 * The <code>GREATER_THAN</code> extends <code>BinaryOperatorTransformFunction</code> to implement the binary
 * operator(>).
 *
 * The results are in boolean format and stored as an integer array with 1 represents true and 0 represents false.
 *
 * SQL Syntax:
 *    columnA > 12
 *    columnA > 12.0
 *    columnA > 'fooBar'
 *
 * Sample Usage:
 *    GREATER_THAN(columnA, 12)
 *    GREATER_THAN(columnA, 12.0)
 *    GREATER_THAN(columnA, 'fooBar')
 *
 */
public class GreaterThanTransformFunction extends BinaryOperatorTransformFunction {

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    super.init(arguments, dataSourceMap);
  }

  @Override
  int getBinaryFuncResult(int result) {
    return (result > 0) ? 1 : 0;
  }

  @Override
  public String getName() {
    return TransformFunctionType.GREATER_THAN.getName();
  }

  @Override
  public int[] transformToIntValuesSV(ProjectionBlock projectionBlock) {
    fillResultArray(projectionBlock);
    return _results;
  }
}