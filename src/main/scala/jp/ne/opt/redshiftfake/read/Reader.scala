package jp.ne.opt.redshiftfake.read

import jp.ne.opt.redshiftfake.ColumnDefinition
import jp.ne.opt.redshiftfake.parse.CopyQuery

class Reader(copyQuery: CopyQuery, columnDefinitions: Seq[ColumnDefinition]) {
  def read()
}
