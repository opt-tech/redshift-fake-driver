package jp.ne.opt.redshiftfake.read

import jp.ne.opt.redshiftfake.ColumnDefinition
import jp.ne.opt.redshiftfake.parse.CopyQuery
import jp.ne.opt.redshiftfake.s3.S3Service

class ReaderService(s3Service: S3Service) {
  def read(copyQuery: CopyQuery, columnDefinitions: Seq[ColumnDefinition]):
}
