The Flink Batch Sink for Pulsar is a custom sink that enables Apache [Flink](https://flink.apache.org/) to write [DataSet](https://ci.apache.org/projects/flink/flink-docs-stable/dev/batch/index.html) to Pulsar.

# Prerequisites

To use this sink, include a dependency for the `pulsar-flink` library in your Java configuration.

# Maven

If you're using Maven, add this to your `pom.xml`:

```xml
<!-- in your <properties> block -->
<pulsar.version>{{pulsar:version}}</pulsar.version>

<!-- in your <dependencies> block -->
<dependency>
  <groupId>org.apache.pulsar</groupId>
  <artifactId>pulsar-flink</artifactId>
  <version>${pulsar.version}</version>
</dependency>
```

# Gradle

If you're using Gradle, add this to your `build.gradle` file:

```groovy
def pulsarVersion = "{{pulsar:version}}"

dependencies {
    compile group: 'org.apache.pulsar', name: 'pulsar-flink', version: pulsarVersion
}
```

# PulsarOutputFormat
### Usage

Please find a sample usage as follows:

```java
        private static final String EINSTEIN_QUOTE = "Imagination is more important than knowledge. " +
                "Knowledge is limited. Imagination encircles the world.";

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarOutputFormat instance
            final OutputFormat<String> pulsarOutputFormat =
                    new PulsarOutputFormat(SERVICE_URL, TOPIC_NAME, wordWithCount -> wordWithCount.toString().getBytes());

            // create DataSet
            DataSet<String> textDS = env.fromElements(EINSTEIN_QUOTE);

            textDS.flatMap(new FlatMapFunction<String, String>() {
                @Override
                public void flatMap(String value, Collector<String> out) throws Exception {
                    String[] words = value.toLowerCase().split(" ");
                    for(String word: words) {
                        out.collect(word.replace(".", ""));
                    }
                }
            })
            // filter words which length is bigger than 4
            .filter(word -> word.length() > 4)

            // write batch data to Pulsar
            .output(pulsarOutputFormat);

            // execute program
            env.execute("Flink - Pulsar Batch WordCount");
        }
```

### Sample Output

Please find sample output for above application as follows:
```
imagination
important
knowledge
knowledge
limited
imagination
encircles
world
```

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchSinkExample.java).
In this example, Flink DataSet is processed as word-count and being written to Pulsar.

### Complete Example Output
Please find sample output for above linked application as follows:
```
WordWithCount { word = important, count = 1 }
WordWithCount { word = encircles, count = 1 }
WordWithCount { word = imagination, count = 2 }
WordWithCount { word = knowledge, count = 2 }
WordWithCount { word = limited, count = 1 }
WordWithCount { word = world, count = 1 }
```

# PulsarCsvOutputFormat
### Usage

Please find a sample usage as follows:

```java
        private static final List<Tuple4<Integer, String, String, String>> employeeTuples = Arrays.asList(
            new Tuple4(1, "John", "Tyson", "Engineering"),
            new Tuple4(2, "Pamela", "Moon", "HR"),
            new Tuple4(3, "Jim", "Sun", "Finance"),
            new Tuple4(4, "Michael", "Star", "Engineering"));

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarCsvOutputFormat instance
            final OutputFormat<Tuple4<Integer, String, String, String>> pulsarCsvOutputFormat =
                    new PulsarCsvOutputFormat<>(SERVICE_URL, TOPIC_NAME);

            // create DataSet
            DataSet<Tuple4<Integer, String, String, String>> employeeDS = env.fromCollection(employeeTuples);
            // map employees' name, surname and department as upper-case
            employeeDS.map(
                    new MapFunction<Tuple4<Integer, String, String, String>, Tuple4<Integer, String, String, String>>() {
                @Override
                public Tuple4<Integer, String, String, String> map(
                        Tuple4<Integer, String, String, String> employeeTuple) throws Exception {
                    return new Tuple4(employeeTuple.f0,
                            employeeTuple.f1.toUpperCase(),
                            employeeTuple.f2.toUpperCase(),
                            employeeTuple.f3.toUpperCase());
                }
            })
            // filter employees who are member of Engineering
            .filter(tuple -> tuple.f3.equals("ENGINEERING"))
            // write batch data to Pulsar
            .output(pulsarCsvOutputFormat);

            // execute program
            env.execute("Flink - Pulsar Batch Csv");

        }
```

### Sample Output

Please find sample output for above application as follows:
```
1,JOHN,TYSON,ENGINEERING
4,MICHAEL,STAR,ENGINEERING
```

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchCsvSinkExample.java).
In this example, Flink DataSet is processed and written to Pulsar in Csv format.


# PulsarJsonOutputFormat
### Usage

Please find a sample usage as follows:

```java
        private static final List<Employee> employees = Arrays.asList(
                    new Employee(1, "John", "Tyson", "Engineering"),
                    new Employee(2, "Pamela", "Moon", "HR"),
                    new Employee(3, "Jim", "Sun", "Finance"),
                    new Employee(4, "Michael", "Star", "Engineering"));

        private static final String SERVICE_URL = "pulsar://127.0.0.1:6650";
        private static final String TOPIC_NAME = "my-flink-topic";

        public static void main(String[] args) throws Exception {

            // set up the execution environment
            final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

            // create PulsarJsonOutputFormat instance
            final OutputFormat<Employee> pulsarJsonOutputFormat = new PulsarJsonOutputFormat<>(SERVICE_URL, TOPIC_NAME);

            // create DataSet
            DataSet<Employee> employeeDS = env.fromCollection(employees);
            // map employees' name, surname and department as upper-case
            employeeDS.map(employee -> new Employee(
                    employee.id,
                    employee.name.toUpperCase(),
                    employee.surname.toUpperCase(),
                    employee.department.toUpperCase()))
            // filter employees which is member of Engineering
            .filter(employee -> employee.department.equals("ENGINEERING"))
            // write batch data to Pulsar
            .output(pulsarJsonOutputFormat);

            env.setParallelism(2);

            // execute program
            env.execute("Flink - Pulsar Batch Json");
        }

        /**
         * Employee data model
         *
         * Note: Properties should be public or have getter function to be visible
         */
        private static class Employee {

            private long id;
            private String name;
            private String surname;
            private String department;

            public Employee(long id, String name, String surname, String department) {
                this.id = id;
                this.name = name;
                this.surname = surname;
                this.department = department;
            }

            public long getId() {
                return id;
            }

            public String getName() {
                return name;
            }

            public String getSurname() {
                return surname;
            }

            public String getDepartment() {
                return department;
            }
        }
```

### Sample Output

Please find sample output for above application as follows:
```
{"id":1,"name":"JOHN","surname":"TYSON","department":"ENGINEERING"}
{"id":4,"name":"MICHAEL","surname":"STAR","department":"ENGINEERING"}
```

### Complete Example

You can find a complete example [here](https://github.com/apache/incubator-pulsar/tree/master/pulsar-flink/src/test/java/org/apache/flink/batch/connectors/pulsar/example/FlinkPulsarBatchJsonSinkExample.java).
In this example, Flink DataSet is processed and written to Pulsar in Json format.
