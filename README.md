# Data Generation

Tool to generate sample graphs for Neo4j - mostly used to synthesize large graphs for testing.

Sample Graph can be generated using [generate_data.py](./datagen/generate_data.py)

```
python3 generate_data.py examples/social-datagen.conf
```

This will generate node and relationship files and output to either:

* csv
* gzip
* parquet (useful for direct graph build via Graph Data Science Library)

If csv or gzip are selected then header files and an importCommand.sh script will be generated that can be run to import via neo4j-admin.

Configuration is done via the yaml file

Valid types are as per admin-import documentation: https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties
