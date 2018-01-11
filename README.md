This library is a Datasource for OpenTSDB in Spark SQL. It connects spark to opentsdb.

It bases on [OpenTSDB4J](), another Project of our Team.

How to use it?

```
  val config = new SparkConf().setAppName("testing provider")  
  val sc = new SparkContext(config)  
  val sqlContext = new SQLContext(sc)  
  
  
  val df = sqlContext  
            .read  
            .format("cn.skydata.opentsdb")  
            .options(Map(....)). //params
            .load()    

```

OR

```
CREATE TEMPORARY TABLE demo
    USING cn.skydata.opentsdb
    OPTIONS (path "episodes.avro") //params
    ……
```  

Due to the limit of OpenTSDB4J, now this version is just a demo for learning Spark SQL DataSource.


What's Next
- 2.0
  - Make some native ways to load Data from OpenTSDB.
- 1.0
  - Make it product-ready and support multi schema.
