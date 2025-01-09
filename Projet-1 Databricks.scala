// Databricks notebook source
// MAGIC %md
// MAGIC **Import the library**

// COMMAND ----------

import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import spark.sqlContext.implicits
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._

// COMMAND ----------

// MAGIC %md
// MAGIC **1-Load data**

// COMMAND ----------


  def getEtudiantsAndProf(path: String): Seq[DataFrame] = {

    val jsonDF = spark.read
      .option("multiline", "true")
      .json(path)

    // DataFrame Etudiants
    val etudiantDF = jsonDF
      .select(explode(col("Etudiants")).as("etudiant"))
      .select(
        col("etudiant.Annee"),
        col("etudiant.Bourse_Exel"),
        col("etudiant.CodeFormation"),
        col("etudiant.IdEtu")
      )
      .withColumn("Universite", split(col("CodeFormation"), "@").getItem(0))
      .withColumn("Niveau", split(col("CodeFormation"), "@").getItem(1))
      

    // DataFrame Profs
    val profDF = jsonDF
      .select(explode(col("Profs")).as("prof"))
      .select(
        col("prof.IdProf"),
        col("prof.CodeFormations"),
        col("prof.Annee")
      )

    Seq(etudiantDF, profDF)
  }
// Utilisation du path
val path = "/FileStore/shared_uploads/luckner446@gmail.com/Data_TP2_MSD-2.txt"
val dfEtudiant = getEtudiantsAndProf(path)(0)
dfEtudiant.show()

// COMMAND ----------

val dfprof = getEtudiantsAndProf(path)(1)
dfprof.show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2- Aggregation Simple

// COMMAND ----------


// Question 1
def SumBourseByNivAndUniv(df: DataFrame): DataFrame = {
  df
    .withColumn("Universite", split(col("CodeFormation"), "@").getItem(0))
    .withColumn("Niveau", split(col("CodeFormation"), "@").getItem(1))
    .groupBy("Universite", "Niveau")
    .agg(sum("Bourse_Exel").alias("SommeBourse"))
    .orderBy("Universite", "Niveau")
}

// COMMAND ----------

// Affichage :
SumBourseByNivAndUniv(dfEtudiant).orderBy("Universite", "Niveau").show

// COMMAND ----------

// Question 2
def CountBourseUnivEachYear(df: DataFrame): DataFrame = {
  df.withColumn("Universite", split(col("CodeFormation"), "@").getItem(0))
    .groupBy("Universite")
    .pivot("Annee")
    .count()
    .orderBy("Universite")
}

// COMMAND ----------

CountBourseUnivEachYear(dfEtudiant).show

// COMMAND ----------

// MAGIC %md
// MAGIC **3-Agrégation avec window partition**

// COMMAND ----------

// Question 1

def TopOnBourseForUnivEachYear(df: DataFrame): DataFrame = {

  val withUnivFormationDF = df
    .withColumn("Universite", split(col("CodeFormation"), "@").getItem(0))
    .withColumn("Formation", split(col("CodeFormation"), "@").getItem(2))

  val windowSpec = Window
    .partitionBy("Universite", "Annee")
    .orderBy(col("Bourse_Exel").desc)

  withUnivFormationDF
    .withColumn("rang", row_number().over(windowSpec))
    .filter(col("rang") === 1)
    .drop("rang")
    .orderBy("Universite", "Annee")
}

// COMMAND ----------

// Affichage

TopOnBourseForUnivEachYear(dfEtudiant).orderBy("Universite", "Annee").show

// COMMAND ----------


// Question 2:

def DiffBetwMinAndNex(df: DataFrame): DataFrame = {
  df.withColumn("universite", split(col("CodeFormation"), "@").getItem(0))
    .withColumn("niveau", split(col("CodeFormation"), "@").getItem(1))
    .withColumn("formation", split(col("CodeFormation"), "@").getItem(2))
    .withColumn("lag", lag("Bourse_Exel", 1).over(Window.partitionBy("Annee").orderBy("Bourse_Exel")))
    .withColumn("dif_between_next", col("Bourse_Exel") - col("lag"))
    .select("Annee", "Bourse_Exel", "CodeFormation", "IdEtu", "universite", 
            "niveau", "formation", "lag", "dif_between_next")
}

// COMMAND ----------

// Affichage
DiffBetwMinAndNex(dfEtudiant).show

// COMMAND ----------

// MAGIC %md
// MAGIC 4-Agrégation Combinée

// COMMAND ----------

// Question 1

def CubeUniversiteAndAnnee(df: DataFrame): DataFrame = {

  val dfWithUniv = df
    .withColumn("Universite", split(col("CodeFormation"), "@")(0))
    
  val cubedDF = dfWithUniv
    .cube("Universite", "Annee")
    .agg(sum("Bourse_Exel").as("Sum_Bourse_Exel"))
    .withColumn("Universite", coalesce(col("Universite"), lit("all_Univ")))
    .withColumn("Annee", coalesce(col("Annee"), lit("all_year")))
    

  cubedDF
}

// COMMAND ----------

 // Affichage
 CubeUniversiteAndAnnee(dfEtudiant).show

// COMMAND ----------

// API SQL 
dfEtudiant.createOrReplaceTempView("selltable")

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT coalesce(Universite, "all_Univ") as Universite,
// MAGIC        coalesce(Annee, "all_year") as Annee,
// MAGIC        SUM(Bourse_Exel) as Sum_Bourse_Exel
// MAGIC        FROM selltable  GROUP BY CUBE (Universite, Annee) ORDER BY Universite

// COMMAND ----------

// MAGIC %md
// MAGIC // En termes de conclusion dans ce graphique, nous pouvons constater pendant toute la période soit de 2017 à 2020 c'est l'université de Thies qui a donné plus de bourse pour une somme de 420 000 alors que l'UCAD n'a donné que 68 000 bourse. de 2017 à 2019, l'octroi de bourse dans les universités à survoler passant de 10 000 à 322 000 et diminue de 2000 bourses  en 2020. Globalement, les universités ont contibué à un hauteur 652 000 bourses pendant toutes les années.

// COMMAND ----------

// MAGIC %md
// MAGIC 5- Cross Data

// COMMAND ----------


def CheckProfRecomp(dfEtudiant: DataFrame, dfProf: DataFrame): DataFrame = {
  val dfEtuExpanded = dfEtudiant
    .withColumn("Université", split(col("CodeFormation"), "@")(0))
    .withColumn("Niveau", split(col("CodeFormation"), "@")(1))
    .withColumn("Formation", split(col("CodeFormation"), "@")(2))

  val dfProfExpanded  = dfProf
    .withColumn("Formation", explode(col("CodeFormations")))
    .withColumn("Université", split(col("Formation"), "@")(0))
    .withColumn("Niveau", split(col("Formation"), "@")(1))
    .withColumn("Formation", split(col("Formation"), "@")(2))
    
  dfProfExpanded
    .join(
      dfEtuExpanded,
      Seq("Formation", "Université", "Niveau", "Annee"),
      "right"
    )
    .select(
      col("Annee"),
      col("IdEtu"),
      col("Université"),
      col("Niveau"),
      col("Bourse_Exel"),
      col("Formation"),
      coalesce(col("IdProf"), lit("ProfS_No_Recompensé")).as("IdProf")
    )
}



// COMMAND ----------

CheckProfRecomp(dfEtudiant, dfprof).show
