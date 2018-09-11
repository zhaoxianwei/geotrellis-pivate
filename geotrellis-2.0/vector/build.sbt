import Dependencies._

name := "geotrellis-vector"
libraryDependencies ++= Seq(
  jts,
  pureconfig,
  sprayJson,
  apacheMath,
  spire,
  scalatest  % Test,
  scalacheck % Test
)

assemblyShadeRules in assembly := {
  val shadePackage = "com.azavea.shaded.demo"
  Seq(
    ShadeRule.rename("shapeless.**" -> s"$shadePackage.shapeless.@1").inAll
  )
}

