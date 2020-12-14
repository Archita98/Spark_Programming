// Databricks notebook source
// /FileStore/tables/Property_data.csv

val data = sc.textFile("/FileStore/tables/Property_data.csv")

// COMMAND ----------

val removeHeader=data.filter(line => !line.contains("Bedrooms"))
removeHeader.take(10)

// COMMAND ----------

// DBTITLE 1,Avg of Price For Different no of bedrooms
val roomrdd=removeHeader.map(x =>(x.split(",")(3).toInt,(1,x.split(",")(2).toDouble)))
roomrdd.take(10)

// COMMAND ----------

val reduced=roomrdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
reduced.take(10)

// COMMAND ----------

val finalrdd=reduced.mapValues(data=>data._2/data._1)
finalrdd.collect()

// COMMAND ----------

for((bedroom,avg)<- finalrdd.collect()) println(bedroom+":"+avg)

// COMMAND ----------

finalrdd.saveAsTextFile("prop.csv")
