
# complie and package
mvn clean package

# submit to execute
hadoop jar target/parquet-0.0.1-SNAPSHOT-jar-with-dependencies.jar split.ParquetSplitTool "split_field" inputPath outputPath

# for mac os
zip -d target/parquet-0.0.1-SNAPSHOT-jar-with-dependencies.jar META-INF/LICENSE

