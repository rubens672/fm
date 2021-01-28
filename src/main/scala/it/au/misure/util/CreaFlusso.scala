package it.au.misure.util

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
//import org.apache.spark.sql.functions.{max, min}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types._
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.Row

import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.io.File
import java.util.HashSet;
import java.util.Set;
import scala.collection.JavaConverters._

import it.au.misure.util.Schemas._
import java.util.Properties
import it.au.misure.util.CreateProperties
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
//import it.au.misure.cli.CommonsCliUtils.Args
import scala.collection.mutable.HashMap
import it.au.misure.util.LoggingSupport
import org.apache.spark.storage.StorageLevel
import com.sun.org.apache.xalan.internal.xsltc.compiler.ValueOf

object CreaFlusso extends LoggingSupport {
  
    def main(args: Array[String]) {
        val commandLineOptions = new CommandLineOptions()
	      val commonsCliUtils = new CommonsCliUtils()
				val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
				val argsObj = commonsCliUtils.getArgsAggregati(commandLine)
						
				log.info("***** Inizio processo Test di Carico - Crea Flussi Misure *****")

				val conf = new SparkConf()
  				.setAppName( "Test di Carico - Crea Flussi Misure" )
  				.set("spark.shuffle.service.enabled", "false")
  				.set("spark.dynamicAllocation.enabled", "false")
  				.set("spark.io.compression.codec", "snappy")
  				.set("spark.rdd.compress", "false")
  				.setMaster( argsObj.master )

				val sc = new SparkContext(conf)
				sc.setLogLevel("ERROR")
				
				val minPartitions =  sc.getConf.get("spark.fm.min.partitions")
				val quartiPDO = sc.getConf.get("spark.fm.quarti")

				val sqlCtx = new SQLContext(sc)
				sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
				sqlCtx.setConf("spark.sql.parquet.compression.codec","uncompressed")
				sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
				sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
				
				val hiveContext =  new SQLContext(sc)

				val sdf = new SimpleDateFormat("yyyyMMddhhmmss")
    
    		//numero di pod da creare (iniziali 57)
    		val numPod = commandLine.getOptionValue(commandLineOptions.creaFM.getOpt).toInt
    		val anno = argsObj.anno
    		val mese = argsObj.mese
    		
    		log.info("*** sc.master: " + sc.master)
				log.info("*** numPod: " + numPod)
				log.info("*** anno: " + anno)
				log.info("*** mese: " + mese)
					
				val query = "podquarti='IT087E00000802'"+
                    " AND annoquarti=" + anno +
                    " AND mesequarti=" + 2 

				val hiveContextPrq = hiveContext.read.parquet(quartiPDO).where(query)
				 
				 
				 
                     
				val flussoDF = hiveContextPrq
      				 .select("coducquarti","podquarti","annoquarti","mesequarti","pivadistributorequarti","codcontrdispquarti","areaquarti",
      				     "pivautentequarti","tipodato_e","tipodato_s","tensione","trattamento_o","potcontrimpl","potdisp","cifreatt","cifrerea","raccolta","validato","potmax","perdita","nomefile","annomesegiornodir","dataelaborazione","time_stamp","giornoquarti",
      				     "e1","e2","e3","e4","e5","e6","e7","e8","e9","e10","e11","e12","e13","e14","e15","e16","e17","e18","e19","e20","e21","e22","e23","e24","e25","e26","e27","e28","e29","e30","e31","e32","e33","e34","e35","e36","e37","e38","e39","e40","e41","e42","e43","e44","e45","e46","e47","e48","e49","e50","e51","e52","e53","e54","e55","e56","e57","e58","e59","e60","e61","e62","e63","e64","e65","e66","e67","e68","e69","e70","e71","e72","e73","e74","e75","e76","e77","e78","e79","e80","e81","e82","e83","e84","e85","e86","e87","e88","e89","e90","e91","e92","e93","e94","e95","e96","e97","e98","e99","e100")
      				 
      	 val schema = flussoDF.schema;
				 
         var millis = System.currentTimeMillis();
    
				 (1 to numPod).map { q =>
  				 val podquarti = "IT087E" + String.valueOf(millis + q).substring(4)
  				 val misure = flussoDF.collect().clone().map{r =>
  				   //tot 225
  				   val array = r.toSeq.toArray 
  				   array(1) = podquarti
  				   array(3) = mese.toInt
  				   array(4) = "000000".concat(String.valueOf(r.getLong(4)))  takeRight 11

    				 Row.fromSeq(array.toSeq)
  				 }.toSeq
  				 
  				val rdd = sc.parallelize(misure)
  				 
  				val dfQS1 = sqlCtx.createDataFrame(rdd, schemaQuartiCreaFlusso)

					dfQS1
							.write
							.format("parquet")
							.mode(SaveMode.Append)
							.partitionBy("annoquarti","mesequarti","pivadistributorequarti", "codcontrdispquarti", "areaquarti")
							.save(quartiPDO)
							log.info("{} - insert misure quarti {} OK", q, podquarti)
				 }
  }
  
  
  
  
  
  
  
  
  
  
  
  
  
  
  
}