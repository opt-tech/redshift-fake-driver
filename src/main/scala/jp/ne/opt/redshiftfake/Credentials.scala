package jp.ne.opt.redshiftfake

/**
 * Represents credentials for COPY/UNLOAD queries.
 */
sealed abstract class Credentials
object Credentials {
  case class WithKey(accessKeyId: String, secretAccessKey: String) extends Credentials

  // TODO: Support other credential types.
  // case class WithRole(awsAccountId: String, roleName: String) extends Credentials
  // case class WithTemporaryToken(temporaryAccessKeyId: String, temporarySecretAccessKey: String, temporaryToken: String) extends Credentials
}
