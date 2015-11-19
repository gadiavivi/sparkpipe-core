# Uncharted Spark Pipeline

[Apache Spark](http://spark.apache.org/) is a powerful tool for distributed data processing. Enhancing and maintaining productivity on this platform involves implementing Spark scripts in a modular, testable and reusable fashion.

The Uncharted Spark Pipeline facilitates expressing individual components of Spark scripts in a standardized way so that they can be:

  - connected in series (or even in a more complex dependency graph of operations)
  - unit tested effectively with mock inputs
  - reused and shared

## Quick Start

Try the pipeline yourself using spark-shell:

```bash
$ spark-shell --packages software.uncharted.sparkpipe:sparkpipe-core:1.0.0
```

```scala
scala> import software.uncharted.sparkpipe.Pipe
scala> Pipe("hello").to(_+" world").run
```

Assuming you have a file named [people.json](https://raw.githubusercontent.com/apache/spark/master/examples/src/main/resources/people.json), read an DataFrame from a file and manipulate it:
```scala
scala> :paste
import software.uncharted.sparkpipe.ops.core._
val people = sqlContext.read.json("people.json")

Pipe(people)
.to(DataFrameOps.renameColumn(Map("age" -> "personAge")))
.to(_.filter("personAge > 21").count)
.run
```
