package io.prophecy.pipelines.livy_sc_airflow_os.config

import pureconfig._
import pureconfig.generic.ProductHint
import io.prophecy.libs._

case class Config(var c1: List[C1] = List(C1(c2 = C2(c3 = "def value here"))))
    extends ConfigBase

object C1 {

  implicit val confHint: ProductHint[C1] =
    ProductHint[C1](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C1(var c2: C2)

object C2 {

  implicit val confHint: ProductHint[C2] =
    ProductHint[C2](ConfigFieldMapping(CamelCase, CamelCase))

}

case class C2(var c3: String)
