logLevel := Level.Warn

import sbt.Resolver

resolvers += Resolver.typesafeRepo("releases")

addSbtPlugin("com.eed3si9n" % "sbt-assembly" % "0.14.5")
