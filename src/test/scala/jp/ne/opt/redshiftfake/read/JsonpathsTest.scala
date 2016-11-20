package jp.ne.opt.redshiftfake.read

import org.scalatest.FlatSpec

class JsonpathsTest extends FlatSpec {
  it should "read json document by index in jsonpaths" in {
    val jsonpaths = new Jsonpaths("""{"jsonpaths":["$['b']","$['a']","$['nested']['c']"]}""")
    val reader = jsonpaths.mkReader("""{"a":42.001,"b":true,"nested":{"c":"hello"}}""")

    assert(reader.valueAt(0) == Some("true"))
    assert(reader.valueAt(1) == Some("42.001"))
    assert(reader.valueAt(2) == Some("hello"))
  }
}
