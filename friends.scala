// Databricks notebook source
// DBTITLE 1,Friends_Data
// /FileStore/tables/FriendsData.csv
 val friends=sc.textFile("/FileStore/tables/FriendsData.csv")


// COMMAND ----------

val removeHeader=friends.filter(line => !line.contains("Age"))
removeHeader.take(10)

// COMMAND ----------

// DBTITLE 1,avg no. of friends
val agerdd = removeHeader.map(x =>( x.split(",")(3).toInt,(1,x.split(",")(2).toInt)))
agerdd.take(10)

// COMMAND ----------

val reduced=agerdd.reduceByKey((x,y)=>(x._1+y._1,x._2+y._2))
reduced.take(10)

// COMMAND ----------

val finalrdd=reduced.mapValues(data=>data._2/data._1)
finalrdd.take(10)

// COMMAND ----------

for((avg,age)<-finalrdd.collect()) 
println(age+":"+avg)

// COMMAND ----------

finalrdd.max()

// COMMAND ----------

// DBTITLE 1,max friends for each age
val friends2rdd = removeHeader.map( x => (x.split(",")(2).toInt, x.split(",")(3).toInt))

// COMMAND ----------

val maxrdd = friends2rdd.reduceByKey(math.max(_,_))
 
maxrdd.collect()

