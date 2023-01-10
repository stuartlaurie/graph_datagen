# Data Generation

Tool to generate sample graphs for Neo4j - mostly used to synthesize large graphs for testing.

## Installation

* You will need to have Python 3 and compatible version of Pip installed.
* Then run `pip3 install -r requirements.txt` to obtain dependencies
* If you do not have a yaml module installed, you may need to run `pip3 install pyyaml`

## Usage

Sample Graph can be generated using [generate_data.py](./generate_data.py)

```
python3 generate_data.py examples/small-test.conf
```

This will generate node and relationship files and output to either:

* csv
* gzip
* parquet (useful for direct graph build via Graph Data Science Library)

If csv or gzip are selected then header files and an importCommand.sh script will be generated that can be run to import via neo4j-admin.

Configuration is done via the yaml file - [here are some example configs](./examples)


### General

* general directory setup/output
* can specify no records/file - setting this will define how many files get created as (total no./records per)


### admin-import

This section gives the parameters that will be written to a shell script in the data generation folder
* **options** - any [option](https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-options) that is available to admin-import can be placed in this section
* **neo4j** - this can be used to add the drop/create database commands to the shell script that will be run using cypher-shell


### nodes

Repeating section for each node to be generated, common settings

* `label`: [label for node]
* `no_to_generate`: [no. of nodes to generate]


### relationships

Repeating section for each relationship to be generated, common settings

* `label`: [label for relationship]
* `no_to_generate`: [no. of nodes to generate]
* `source_node_label`: [label of source node]
* `target_node_label`: [User of target node]


### properties

Each node/relationship block can have repeating properties section, properties can have

* `name`: [name of property]
* `type`: [type of property] - each type has it's own set of configuration/behavior

**int**

will generate random int between lower and upper values

* `lower`: [lowest int]
* `upper`: [highest int]

**date**

will generate random date between lower and upper values - split down to ymd to avoid pesky US/European dates

* `lower`:
-   `year`: 2022
-   `month`: 1
-   `day`: 1
* `upper`:
-   `year`: 2023
-   `month`: 1
-   `day`: 1

**list**

will randomly select a value from the list and pass to admin-import as a string

* `values`: [list of values to select from]

Eventually will support all valid types are as per admin-import documentation: https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties, with exception that you can specify a list as input
