# data-demo
load and query amazon public data pyspark

### Description
This is a simple docker demo, includes the following api
- copy an irs990 index file from Amazon public dataset. 
- view sample data
- view columns definition
- do simple query on data

### Note
This is just a simple demo with limited feature. Parquet file loading is not supported. If you wish to load parquet file or directly load file from AWS S3, you need to install hadoop

### Setup
copy files to your local, then run `docker-compose up`

### How to use
Once docker is up, open a web browser, go to `localhost:5000`. The home page will guide you how to call API. You could directly invoke api by click the link on the page or use tools like `curl` and `Postman`

### APIs
| API | Description |
|---|---|
|localhost:5000/api/irs/ingest | Load data |
|localhost:5000/api/irs/sample| Retrieve 20 rows of sample data|
|localhost:5000/api/irs/column| Get column name and datatype|
|localhost:5000/api/irs/data| Query data. Two parameters needed for query `q` and `max`<br> -`q`: data query<br>-`max`:Maximum number of records retrieved|

### Query example
| Query | Description |
|---|---|
|localhost:5000/api/irs/data | Retrieve all records |
|localhost:5000/api/irs/data?q={"tax_period":{"eq":"201106"}}&max=10| Retreive 10 records which tax_period equal to "201106"|
|localhost:5000/api/irs/data?q={"tax_period":{"in":["201106","201109"]}}| Retrieve records which tax_period equal to "201106" or "201109"|

### Query operators
This demo provides very basic operator which can be used in query<br>

| Operator | Description | Example |
|---|---|---|
|`eq`|=| {"tax_period": {"eq":"201106"}} |
|`lt`|<| {"tax_period": {"lt":"201106"}}|
|`gt`|>| {"tax_period": {"gt":"201106"}}|
|`in`|Match any value in array| {"tax_period": {"in":["201106","201109"]}} |



