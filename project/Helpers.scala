import sbt._

object Helpers {
  def compileScope(modules: ModuleID*): Seq[ModuleID] = modules.map(_ % Compile)
  def providedScope(modules: ModuleID*): Seq[ModuleID] = modules.map(_ % Provided)
  def testScope(modules: ModuleID*): Seq[ModuleID] = modules.map(_ % Test)
}
