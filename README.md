# Automating AWS Data Migration Service (DMS) Tasks

## Steps
### Create JSON Configuration files


### Create DMS Tasks

### `include.csv` layout
Each record in this file should have one of these layouts. 

#### #1 For Partitioned tables
Following values should appear in the order:
- `Schema`
- `Table`
- `Partition column`
- `Operator`
  - `BETWEEN`
  - `GTE`
  - `STE`
- `lower bound/upper bound`  

**Sample**
```sh
HR,EMPLOYEES,JOINING_DATE,BETWEEN,2020-01-01,2020-01-31
HR,EMPLOYEES,JOINING_DATE,STE,2020-01-01
HR,EMPLOYEES,JOINING_DATE,GTE,2020-01-31
```
#### #2 For non-partitioned tables
- `Schema`
- `Table`

#### #3 For Partitioned tables - AUTO
- `Schema`
- `Table`
- `partitions-auto`

**Sample**
```sh
HR,SALES,partitions-auto
```
For this input, generated JSON will look like this:  
```json
{
   "rules": [{
            "rule-type": "selection",
            "rule-id": "1",
            "rule-name": "1",
            "object-locator": {
                "schema-name": "HR",
                "table-name": "SALES"
            },
            "rule-action": "include"
        },
        {
            "rule-type": "table-settings",
            "rule-id": "2",
            "rule-name": "2",
            "object-locator": {
                "schema-name": "HR",
                "table-name": "SALES"
            },
            "parallel-load": {
                "type": "partitions-auto"
            }
        }
     ]
}
```
****
