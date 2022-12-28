# Data Generation

Sample Graph can be generated using [generate_data.py](./datagen/generate_data.py)

```
python3 generate_data.py -u 280737632 -f 1547592072 -d twitter300m
```

This generates will generate CSV and an importCommand.sh script that can be used to load the data via neo4j-admin import

* -u specifies the # of Users
* -f specifies the # FOLLOWS relationship types
