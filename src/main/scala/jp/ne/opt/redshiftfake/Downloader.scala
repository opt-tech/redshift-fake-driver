package jp.ne.opt.redshiftfake

import jp.ne.opt.redshiftfake.s3.S3Service

sealed abstract class Downloader

object Downloader {
  class S3Downloader(s3Service: S3Service) extends Downloader {
    def downloadAllAsString(bucket: String, prefix: String): String = {
      val summaries = s3Service.lsRecurse(bucket, prefix)
      summaries.map { summary =>
        s3Service.downloadAsString(bucket, summary.getKey)
      }.mkString
    }
  }
}
