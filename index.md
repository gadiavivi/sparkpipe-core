---
layout: default
---

# Uncharted Spark Pipeline &nbsp;[![Build Status](https://travis-ci.org/unchartedsoftware/sparkpipe-core.svg?branch=master)](https://travis-ci.org/unchartedsoftware/sparkpipe-core)&nbsp;[![Coverage Status](https://coveralls.io/repos/unchartedsoftware/sparkpipe-core/badge.svg?branch=master&service=github)](https://coveralls.io/github/unchartedsoftware/sparkpipe-core?branch=master)

[Apache Spark](http://spark.apache.org/) is a powerful tool for distributed data processing. Enhancing and maintaining productivity on this platform involves implementing Spark scripts in a modular, testable and reusable fashion.

The Uncharted Spark Pipeline facilitates expressing individual components of Spark scripts in a standardized way so that they can be:

  - connected in series (or even in a more complex dependency graph of operations)
  - unit tested effectively with mock inputs
  - reused and shared

## Quick Start

Try the pipeline yourself using spark-shell:

{% highlight bash %}
$ spark-shell --packages software.uncharted.sparkpipe:sparkpipe-core:0.9.3
{% endhighlight %}

{% highlight scala %}
scala> import software.uncharted.sparkpipe.Pipe
scala> Pipe("hello").to(_+" world").run
{% endhighlight %}

Assuming you have a file named [people.json](https://raw.githubusercontent.com/apache/spark/master/examples/src/main/resources/people.json), read a DataFrame from a file and manipulate it:

{% highlight scala %}
scala> :paste
import software.uncharted.sparkpipe.{ops => ops}
val people = sqlContext.read.json("people.json")

Pipe(people)
.to(ops.core.dataframe.renameColumns(Map("age" -> "personAge")))
.to(_.filter("personAge > 21").count)
.run
{% endhighlight %}

## Included Operations

The Uncharted Spark Pipeline comes bundled with core operations which perform a variety of useful tasks, and are intended to serve as aids in implementing more domain-specific operations.

Core operations fall into the following categories:

### RDD Operations

- [General Operations]({{ "/docs/0.9.3/#software.uncharted.sparkpipe.ops.core.rdd.package" | prepend: site.baseurl }})

### DataFrame Operations

- [General Operations]({{ "docs/0.9.3/#software.uncharted.sparkpipe.ops.core.dataframe.package" | prepend: site.baseurl }})
- [Input/Output]({{ "docs/0.9.3/#software.uncharted.sparkpipe.ops.core.dataframe.io.package" | prepend: site.baseurl }})
- [Numeric Data]({{ "docs/0.9.3/#software.uncharted.sparkpipe.ops.core.dataframe.numeric.package" | prepend: site.baseurl }})
- [Temporal Data]({{ "docs/0.9.3/#software.uncharted.sparkpipe.ops.core.dataframe.temporal.package" | prepend: site.baseurl }})
- [Textual Data]({{ "docs/0.9.3/#software.uncharted.sparkpipe.ops.core.dataframe.text.package" | prepend: site.baseurl }})

### DataSet Operations

*Coming soon!*
