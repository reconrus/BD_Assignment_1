# Assignment №1. MapReduce. Simple Search Engine
## Our team:
Vyacheslav Yasrebov, Maxim Popov, Ilia Mazan, Timerlan Nasyrov
## To access cluster (or use Putty)
```
ssh istanbul@10.90.138.32
```
## To see usage
```
/hadoop/bin/hadoop jar search_engine.jar 
```

```
Usage:	Indexer INPUT_FOLDER 
		IDF_OUTPUT_FOLDER is 'idf_output'
		TF_IDF_OUTPUT_FOLDER is 'tf_idf_output'

Usage:	Query   NUMBER_OF_RESULTS QUERY_TEXT
		TF_IDF_INPUT_FOLDER is 'tf_idf_output'
		RESULT_FOLDER is 'result'

Example: /hadoop/bin/hadoop jar search_engine.jar Indexer Dataset/
```

## To get file from hdfs:
```
/hadoop/bin/hdfs dfs -get [path_to_file]
```
