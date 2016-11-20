package jp.ne.opt.redshiftfake

import org.scalatest.TestSuite

trait CIOnly { self: TestSuite =>
  def skiplIfLocalEnvironment(): Unit = {
    if (sys.env.get("CI") != Some("true")) {
      cancel("run only in CI")
    }
  }
}
