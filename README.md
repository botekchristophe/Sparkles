# Sparkles
Data engineering made easy with Spark 3

#Features planned for v0.1

- a basic local file system implementation allowing for development and testing
- read and write for insert, upsert, scd1 and scd2 for DeltaLake tables
- read and write for insert, upsert, scd1 and scd2 for SQLLite tables
- A first version of an Extract, Transform and Load Class
- A first version of a simple scheduler reading jobs to run from a config file and resolving execution order
based on dependencies
- a simple service locator allowing to abstract the FileSystem layer

#Work in progress

- Add automatic build and packaging
- Improve unit testing coverage

```commandline
[info] Statement coverage.: 49.77%
[info] Branch coverage....: 50.00%
[info] Coverage reports completed
[error] Coverage is below minimum [49.77% < 90.0%]
[info] All done. Coverage was [49.77%]
[success] Total time: 6 s, completed 24-Oct-2020 9:13:18 PM
```
