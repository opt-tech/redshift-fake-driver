package jp.ne.opt.redshiftfake

/**
 * Global variables should be avoided in general,
 * but s3 endpoint must be provided as global variable to parse datasource of COPY queries.
 */
object Global {
  var endpoint: String = ""
}
