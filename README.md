# Uncharted Spark Pipeline &nbsp;[![Build Status](https://travis-ci.org/unchartedsoftware/sparkpipe-core.svg?branch=master)](https://travis-ci.org/unchartedsoftware/sparkpipe-core)&nbsp;[![Coverage Status](https://coveralls.io/repos/unchartedsoftware/sparkpipe-core/badge.svg?branch=master&service=github)](https://coveralls.io/github/unchartedsoftware/sparkpipe-core?branch=master)

> [http://unchartedsoftware.github.io/sparkpipe-core](http://unchartedsoftware.github.io/sparkpipe-core)

[Apache Spark](http://spark.apache.org/) is a powerful tool for distributed data processing. Enhancing and maintaining productivity on this platform involves implementing Spark scripts in a modular, testable and reusable fashion.

The Uncharted Spark Pipeline facilitates expressing individual components of Spark scripts in a standardized way so that they can be:

  - connected in series (or even in a more complex dependency graph of operations)
  - unit tested effectively with mock inputs
  - reused and shared

**Note:** Sparkpipe is only compatible with Spark 2+ as of Release 0.9.8. If you're looking for Spark 1.x compatibility, please see Release 0.9.7 and lower.

## Quick Start

Try the pipeline yourself using spark-shell:

```bash
$ spark-shell --packages software.uncharted.sparkpipe:sparkpipe-core:0.9.8
```

```scala
scala> import software.uncharted.sparkpipe.Pipe
scala> Pipe("hello").to(_+" world").run
```

Assuming you have a file named [people.json](https://raw.githubusercontent.com/apache/spark/master/examples/src/main/resources/people.json), read a DataFrame from a file and manipulate it:
```scala
scala> :paste
import software.uncharted.sparkpipe.Pipe
import software.uncharted.sparkpipe.ops

Pipe(sparkSession)
.to(ops.core.dataframe.io.read("people.json", "json"))
.to(ops.core.dataframe.renameColumns(Map("age" -> "personAge")))
.to(_.filter("personAge > 21").count)
.run
```

## Advanced Usage

### Optional Stages

```scala
scala> import software.uncharted.sparkpipe.Pipe
scala> Pipe("hello").maybeTo(None).run // == "hello"
scala> Pipe("hello").maybeTo(Some(a => a+" world")).run // == "hello world"
```

### Branching

```scala
import software.uncharted.sparkpipe.Pipe

val oneInjest = Pipe("some complex data injest pipeline")

val transform = oneInjest.to(_.toUpperCase())

val toHdfs = oneInjest.to(in => {
  // convert to parquet and send to HDFS
})

transform.run
toHdfs.run
// or
Pipe(transform, toHdfs).run
```

### Merging

```scala
import software.uncharted.sparkpipe.Pipe

val oneInjest = Pipe("some complex data injest pipeline")
val anotherInjest = Pipe("another complex data injest pipeline")

// You can merge up to 5 pipes this way
val transform = Pipe(oneInjest, anotherInjest).to(in => {
  val oneOutput = in._1
  val twoOutput = in._2
  oneOutput + " and " + twoOutput
})
.run
```

### Caching

```scala
import software.uncharted.sparkpipe.Pipe

val oneInjest = Pipe("some complex data injest pipeline")
val anotherInjest = Pipe("another complex data injest pipeline")

// merge and run
val transform = Pipe(oneInjest, anotherInjest).to(in => {
  val oneOutput = in._1
  val twoOutput = in._2
  oneOutput + " and " + twoOutput
})
.run

// at this point, the output of every stage of every Pipe is cached
oneInjest.run // <- this will return a reference to the same String as the one used inside transform!
              // this is useful, so that you can cache and reuse the same RDDs/DataFrames in multiple Pipes

// want to clear the cache?
oneInjest.reset
oneInjest.run // <- this is a new copy of the string "some complex data injest pipeline"
```

## Included Operations

The Uncharted Spark Pipeline comes bundled with core operations which perform a variety of useful tasks, and are intended to serve as aids in implementing more domain-specific operations.

For more information, check out the [docs](http://unchartedsoftware.github.io/sparkpipe-core).
