/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ProjectingInternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, MergeRows, Project, WriteDelta}
import org.apache.spark.sql.catalyst.plans.logical.MergeRows.Keep
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.RowDeltaUtils
import org.lance.spark.LancePositionDeltaOperation
import org.lance.spark.utils.SparkUtil

import scala.collection.JavaConverters._

class UpdateColumnsExtractor(session: SparkSession) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = plan transformDown {
    case wd: WriteDelta if SparkUtil.rewriteColumns(session) =>
      try {
        wd.operation match {
          case op: LancePositionDeltaOperation =>
            val targetAttrs = wd.table.output
            val targetColOrdinals = wd.projections.rowProjection match {
              case Some(ProjectingInternalRow(_, colOrdinals: Seq[Int])) => colOrdinals
              case _ => Seq.empty
            }

            val updatedColumns = wd.query match {
              case m: MergeRows =>
                extractMergeRowsUpdatedColumns(m, targetColOrdinals, targetAttrs)

              case p: Project =>
                extractProjectUpdatedColumns(p, targetColOrdinals, targetAttrs)

              case _ => Seq.empty
            }

            op.setUpdatedColumns(updatedColumns.distinct.asJava)

          case _ =>
        }

      } catch {
        case e: Exception => {
          throw new RuntimeException(
            "Error when extract updated columns, please set `" + SparkUtil.REWRITE_COLUMNS + "` to `false` do disable lance's RewriteColumns mode",
            e)
        }
      }

      wd
  }

  /**
   * Extracts the names of columns that are updated from a MergeRows logical plan.
   * It checks the merge instructions for update operations and compares target attributes with the output expressions
   * to identify changed columns.
   * @param mergeRows The MergeRows logical plan to process
   * @param targetColOrdinals The ordinals of columns to consider
   * @param targetAttrs The target table attributes
   * @return Sequence of updated column names
   */
  private def extractMergeRowsUpdatedColumns(
      mergeRows: MergeRows,
      targetColOrdinals: Seq[Int],
      targetAttrs: Seq[Attribute]): Seq[String] = {
    val actions =
      mergeRows.matchedInstructions ++ mergeRows.notMatchedInstructions ++ mergeRows.notMatchedBySourceInstructions

    val operationColIndex =
      mergeRows.output.indexWhere(_.name.equals(RowDeltaUtils.OPERATION_COLUMN))

    actions.flatMap {
      case Keep(_, output) =>
        val operation = output(operationColIndex).asInstanceOf[Literal].value
        operation match {
          case RowDeltaUtils.UPDATE_OPERATION =>
            // Only check update operation

            targetAttrs.zipWithIndex.flatMap {
              case (attr, idx) if !attr.semanticEquals(output(targetColOrdinals(idx))) =>
                Some(attr.name)
              case _ => None
            }

          case _ => Seq.empty
        }
      case _ => Seq.empty
    }
  }

  /**
   * Extracts the names of columns that are updated from a Project logical plan.
   * It checks the operation column to determine if it's an update and compares target attributes with the project expressions
   * to identify changed columns.
   * @param project The Project logical plan to process
   * @param targetColOrdinals The ordinals of columns to consider
   * @param targetAttrs The target table attributes
   * @return Sequence of updated column names
   */
  private def extractProjectUpdatedColumns(
      project: Project,
      targetColOrdinals: Seq[Int],
      targetAttrs: Seq[Attribute]): Seq[String] = {
    val projections = project.projectList
    val operationColIndex = projections.indexWhere(_.name.equals(RowDeltaUtils.OPERATION_COLUMN))

    if (operationColIndex == -1) return Seq.empty

    val operation = projections(operationColIndex).eval()
    operation match {
      case RowDeltaUtils.UPDATE_OPERATION =>
        // Only check update operation

        targetAttrs.zipWithIndex.flatMap {
          case (attr, idx) if !attr.semanticEquals(projections(targetColOrdinals(idx))) =>
            Some(attr.name)
          case _ => None
        }

      case _ => Seq.empty
    }
  }

}
