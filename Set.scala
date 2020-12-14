// Databricks notebook source
val julyrdd=sc.textFile("/FileStore/tables/nasa_july-1.tsv")
val augustrdd=sc.textFile("/FileStore/tables/nasa_august-1.tsv")

// COMMAND ----------

// DBTITLE 1,Union
val unionrdd=julyrdd.union(augustrdd)

// COMMAND ----------

unionrdd.take(20)

// COMMAND ----------

def headerRemover(line:String): Boolean = !(line.contains("bytes"))

// COMMAND ----------

unionrdd.filter(x => headerRemover(x)).take(2)

// COMMAND ----------

val t1=unionrdd.filter(x => (x(5)==0 && (x(6) > 200)))
                       
t1.take(5)                     

// COMMAND ----------

// DBTITLE 1,Intersection
val i =julyrdd.intersection(augustrdd)
i.collect()

// COMMAND ----------

val nasaj=julyrdd.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasaa=augustrdd.map(x=>x.split("\t")(0)).filter(x=>x!="host")
val nasacomm=nasaj.intersection(nasaa)
nasacomm.count()

// COMMAND ----------

val data = List("tushar","goyal","tushar","sir","regex")

// COMMAND ----------

val datardd=sc.parallelize(data)

// COMMAND ----------

datardd.count()

// COMMAND ----------

datardd.countByValue()

// COMMAND ----------

val data2=List(1,2,3,4,5)

val data2rdd=sc.parallelize(data2)

// COMMAND ----------

val productrdd=data2rdd.reduce( (x,y)=> x*y)
productrdd

// COMMAND ----------

val numrdd=sc.textFile("/FileStore/tables/numberData.csv")

// COMMAND ----------

numrdd.collect()

// COMMAND ----------

// DBTITLE 1,Sum of Primes
val header=numrdd.first()
val rem=numrdd.filter(x=> x != header)
rem.collect()

// COMMAND ----------


    def isPrime2(i :Int) : Boolean = {
  if (i <= 1)
       false
     else if (i == 2)
       true
     else
       !(2 to (i-1)).exists(x => i % x == 0)
   }


// COMMAND ----------

val numberRdd = rem.map(x => x.toInt)
numberRdd.take(10)

// COMMAND ----------

numberRdd.filter(i => (isPrime2(i))).sum()
