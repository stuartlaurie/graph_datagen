# Data Generation

Tool to generate sample graphs for Neo4j - mostly used to synthesize large graphs for testing.

## Installation

* You will need to have Python 3 and compatible version of Pip installed.
* Then run `pip3 install -r requirements.txt` to obtain dependencies
* If you do not have a yaml module installed, you may need to run `pip3 install pyyaml`

## Usage

Sample Graph can be generated using [generate_graph_data.py](./generate_graph_data.py)

```
python3 generate_graph_data.py examples/small-test.conf
```

This will generate node and relationship files and output to either:

* csv
* gzip
* parquet (useful for direct graph build via Graph Data Science Library)

If csv or gzip are selected then header files and an importCommand.sh script will be generated that can be run to import via neo4j-admin.

Configuration is done via the yaml file - [here are some example configs](./examples)


### General

* general directory setup/output
* `records_per_file`: [no records per file] - setting this will define how many files get created as (total no./records per) `something to play with for larger datasets` - supports both 1000000 and 1,000,000 formats
* `df_row_limit`: [max no. rows in dataframe] - helps with memory management when creating files with larger no. records, will create limit rows and append to file


### admin-import

This section gives the parameters that will be written to a shell script in the data generation folder
* **options** - any [option](https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-options) that is available to admin-import can be placed in this section
* **neo4j** - this can be used to add the drop/create database commands to the shell script that will be run using cypher-shell


### nodes

Repeating section for each node to be generated, common settings

* `label`: [label for node]
* `no_to_generate`: [no. of nodes to generate] - supports both 1000000 and 1,000,000 formats
* `start_id`: optional - give a start id that will be incremented on - useful when generating incremental sets e.g start numbering at this value (if not specified, numbering starts at 1)

#### additional labels

Repeating section for each additional label to specify, useful for e.g. generating additional occasional labels like 'flagged'

* `labels`:
    * `name`: [name for section, gets used in df column names]
    * `values`: [list of labels to select from]
    * `probability`: [list of probabilities for the values] - if not specified defaults to random    

### relationships

Repeating section for each relationship to be generated, common settings

* `label`: [label for relationship]
* `no_to_generate`: [no. of relationships to generate] - supports both 1000000 and 1,000,000 formats
* `ratio_to_generate`: [ratio of relationships to generate] - can be used instead of `no_to_generate` will apply ratio to `source label` value `no_to_generate`
* `source_node_label`: [label of source node]
* `target_node_label`: [User of target node]
* `rel_multiplier`: [will take random value between lower/upper and generate that no. of relationhships a source/target]
    * `lower`: 1
    * `upper`: 28
* `start_id`: optional - give a start id that will be incremented on - useful when generating incremental sets e.g start numbering at this value (if not specified, numbering starts at 1)

### properties

Each node/relationship block can have repeating properties section, properties can have

* `name`: [name of property]
* `type`: [type of property] - each type has it's own set of configuration/behavior
* `output_type`: [data type to be used in admin-import header] - used when generating e.g. email as while we want an email to be generated, the type for admin-import is string. Also applies to lists where its random selection of a value.

**int**

will generate random int between lower and upper values

* `lower`: [lowest int]
* `upper`: [highest int]

**float**

will generate random float between lower and upper values with no. decimal places defined by precision

* `lower`: [lowest float]
* `upper`: [highest float]
* `precision`: [no decimal places]

**date**

will generate random date between lower and upper values - split down to ymd to avoid pesky US/European dates

* `lower`:
    * `year`: 2022
    * `month`: 1
    * `day`: 1
* `upper`:
    * `year`: 2023
    * `month`: 1
    * `day`: 1

**datetime/epoch**

will generate random datetime between lower and upper values - split down to ymd to avoid pesky US/European dates - can be output either as datetime string (default) or if using epoch use `output_type: int`

* `lower`:
    * `year`: 2022
    * `month`: 1
    * `day`: 1
    * `hour`: 1
    * `second`: 1
    * `minute`: 1        
* `upper`:
    * `year`: 2023
    * `month`: 1
    * `day`: 1
    * `hour`: 1
    * `second`: 1
    * `minute`: 1        

**list**

will randomly select a value from the list and pass to admin-import - if not string need to define type for admin-import using `output_type` field

* `values`: [list of values to select from]
* `probability`: [list of probabilities for the values] - if not specified defaults to random

**name**

uses `fake.name()` [Faker](https://faker.readthedocs.io/en/master/index.html) function to generate random data

**email**

uses `fake.company_email()` [Faker](https://faker.readthedocs.io/en/master/index.html) function to generate random data

**phone**

uses `fake.phone_number()` [Faker](https://faker.readthedocs.io/en/master/index.html) function to generate random data

**ssn**

uses `fake.ssn()` [Faker](https://faker.readthedocs.io/en/master/index.html) function to generate random data

**ip**

uses `fake.ipv4()` [Faker](https://faker.readthedocs.io/en/master/index.html) function to generate random data

### Task List

* support all valid types are as per admin-import documentation: https://neo4j.com/docs/operations-manual/current/tools/neo4j-admin/neo4j-admin-import/#import-tool-header-format-properties
