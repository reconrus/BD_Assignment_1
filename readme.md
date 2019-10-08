To access cluster (or use Putty)
```
ssh istanbul@10.90.138.32
```

To see usage
```
/hadoop/bin/hadoop jar search_engine.jar 
```

```
Usage:	Indexer INPUT_FOLDER [IDF_OUTPUT_FOLDER TF_IDF_OUTPUT_FOLDER]
		Default for IDF_OUTPUT_FOLDER is 'idf_output'
		Default for TF_IDF_OUTPUT_FOLDER is 'tf_idf_output'

Usage:	Query   NUMBER_OF_RESULTS QUERY_TEXT
		Default for TF_IDF_INPUT_FOLDER is 'tf_idf_output'
		Default for RESULT_FOLDER is 'result'

Example: /hadoop/bin/hadoop jar search_engine.jar Indexer Dataset/
```

To get file from hdfs:
```
/hadoop/bin/hdfs dfs -get [path_to_file]
```
