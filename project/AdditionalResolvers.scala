import sbt._

object AdditionalResolvers {
  val resolvers = Seq(
    Resolver.sonatypeRepo("releases"),
    Resolver.typesafeRepo("releases")
  )
}