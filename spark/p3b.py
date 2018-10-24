spark=SparkSession.builder
from pyspark import SparkContext 
from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
escuela=sqlContext.read.csv("/home/alejandra/Documents/5toA~o_1erSemestre/BigDataAnalytics_ICOM5995/exam1/exam1-sp17-bigdata-desc-master/hive/escuelasPR.csv")
escuela.show()
escuelaDF = escuela.selectExpr("_c0 as Region","_c1 as Distrito", "_c2 as Ciudad","_c3 as school_ID","_c4 as NombreEscuela","_c5 as Nivel","_c6 as numSerieEscuela")
escuelaDF.show()
areciboRegion = escuelaDF.filter("Region='Arecibo'")
areciboRegion.show()
distritoYciudad = areciboRegion.groupBy("Distrito","Ciudad")
distritoYciudad.count().show()

