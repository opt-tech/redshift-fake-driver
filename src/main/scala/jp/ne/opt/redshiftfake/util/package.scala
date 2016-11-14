package jp.ne.opt.redshiftfake

package object util {
  def using[A <: AutoCloseable, B](resource: A)(f: A => B) = try {
    f(resource)
  } finally {
    resource.close()
  }
}
