// Databricks notebook source
var a = 1

 a=10

// COMMAND ----------

val c=100

c=10

// COMMAND ----------

lazy val a=10

// COMMAND ----------

// DBTITLE 1,Lazy Variable
lazy val variable_lazy={println("hello world");5}

variable_lazy

// COMMAND ----------

variable_lazy

// COMMAND ----------

lazy val sum=10+b

val b=9

println(sum)

// COMMAND ----------

lazy var a=10

// COMMAND ----------

val `my name is tushar`="goyal"

// COMMAND ----------

val `import` = 10

print(`import`)

// COMMAND ----------

// DBTITLE 1,Function
def square(a:Int):Int={
  
   a*a
}

square(2)

// COMMAND ----------

val a="\t\n\u03bb"

// COMMAND ----------

def add(a:Int,b:Int)=a+b
println(add(6,7))

// COMMAND ----------

val `void` = 100

val `false`=true

val `return` = 90

if(`false`) `void` else `return`

// COMMAND ----------

def square(a:Int):Int={
  a*a
}

def sq2(y:Int, functions:Int=>Int):Int={
  
  functions(y)
}

sq2(2,square)

// COMMAND ----------

// DBTITLE 1,List
val new1=List(1,2,3,4,5,6,7)

new1

// COMMAND ----------

new1.reverse

// COMMAND ----------

// DBTITLE 1,Array
var new2=Array(1,2,3)

// COMMAND ----------

new2(0)=100

new2

// COMMAND ----------

// DBTITLE 1,RDD
val data=List(1,2,3,4,5)

val creationRDD = sc.parallelize(data)

// COMMAND ----------

creationRDD.collect()

// COMMAND ----------

creationRDD.partitions.size

// COMMAND ----------

val rddParttition=sc.parallelize(List(1,2,3,4),2)

// COMMAND ----------

rddParttition.count

// COMMAND ----------

rddParttition.partitions.size

// COMMAND ----------

// DBTITLE 1,Map
val maprdd = rddParttition.map( x => x * x * x )
maprdd.collect()

// COMMAND ----------

// DBTITLE 1,Filter
maprdd.filter(x=>x%2==0).collect()

// COMMAND ----------

val mainrdd=sc.parallelize(List("hey","hello","tushar","sir"))

mainrdd.collect()

// COMMAND ----------

mainrdd.map(x=>x.split(",")).collect()

// COMMAND ----------

mainrdd.flatMap(x=>x.split(",")).collect()

// COMMAND ----------

val rdd0=sc.parallelize(Array("one","two","three","one","two"))

// COMMAND ----------

val keyrdd=rdd0.map(x=>(x,1))

keyrdd.collect()

// COMMAND ----------

// DBTITLE 1,ReduceByKey
keyrdd.reduceByKey(_+_).collect(spark_)
