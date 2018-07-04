package jp.ne.opt.redshiftfake

/**
 * Represents credentials for COPY/UNLOAD commands.
 */
sealed abstract class Credentials
object Credentials {
  case class WithKey(accessKeyId: String, secretAccessKey: String) extends Credentials
  case class WithRole(roleName: String) extends Credentials

  // TODO: Support other credential types.
  // case class WithTemporaryToken(temporaryAccessKeyId: String, temporarySecretAccessKey: String, temporaryToken: String) extends Credentials
}
