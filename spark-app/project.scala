// Build definition for scala-cli (https://scala-cli.virtuslab.org).
// Spark is "compileOnly": spark-submit provides the jars at runtime, so the packaged
// jar contains only our classes.
//> using scala 2.13.16
//> using compileOnly.dep org.apache.spark::spark-sql:4.0.4
//> using compileOnly.dep org.apache.spark::spark-graphx:4.0.4
//> using options -deprecation -feature
