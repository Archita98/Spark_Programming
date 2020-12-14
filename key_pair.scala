// Databricks notebook source
val data = sc.textFile("/FileStore/tables/airports.text")

// COMMAND ----------

data.collect()

// COMMAND ----------

// DBTITLE 1,Key Value
val datardd= data.map(line =>( line.split(",")(1),  line.split(",")(3)))
datardd.take(20)

// COMMAND ----------

val except_Canada=datardd.filter(x => x._2 != "\"Canada\"")
except_Canada.take(20)

// COMMAND ----------

val listd=Array("Tushar 1998","Archita 1997")


// COMMAND ----------

val reqd=listd.map(line => (line.split(" ")(0), line.split(" ")(1).toInt))


// COMMAND ----------

reqd.mapValues(x => x+10).take(2)

// COMMAND ----------

val key = data.map(line => (line.split(",")(1), line.split(",")(11).toLowerCase))
key.take(10)
