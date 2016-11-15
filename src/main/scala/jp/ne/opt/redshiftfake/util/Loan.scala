package jp.ne.opt.redshiftfake.util

class Loan[A <: AutoCloseable] private (resource: A) {
  def foreach[B](f: A => B): B = try {
    f(resource)
  } finally {
    resource.close()
  }
}

object Loan {
  def using[A <: AutoCloseable, B](resource: A)(f: A => B) = try {
    f(resource)
  } finally {
    resource.close()
  }

  def apply[A <: AutoCloseable](resource: A) = new Loan(resource)
}
