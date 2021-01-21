package it.au.misure.util

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.SQLContext
import scala.xml.XML
import scala.xml.Attribute
import scala.xml.Text
import scala.xml.Null
import collection.mutable.ArrayBuffer
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import java.text.SimpleDateFormat
import java.sql.Timestamp
import java.io.File
import java.io.FilenameFilter
import java.util.Date
import java.util.Calendar
import java.util.GregorianCalendar
import java.io.FileNotFoundException
// import java.util.List
import java.util.ArrayList
//import java.util.Iterator
import org.apache.spark.sql.execution.debug._
import org.apache.log4j.{Level, LogManager}
import scala.util.Try
import scala.util.control.Breaks._
import javax.xml.transform.stream.StreamSource
import javax.xml.transform.Source
import javax.xml.validation.SchemaFactory
import javax.xml.XMLConstants
import javax.xml.validation.Validator
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet
import java.sql.PreparedStatement
import it.au.misure.util.Schemas._
import java.util.Properties
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
import org.apache.log4j.Logger

//import org.apache.commons.io.input.BOMInputSteam 
//import scala.io.Source
import java.io.FileInputStream
import scala.xml.pull.XMLEventReader

/**
 * Questa è un oggetto di test utilizzato in fase di sviluppo per provare varie porzioni di codice utilizzate nello sviluppo dei vari processi.
 * 
 *  {{{
*  import ComplexImplicits._
*  val c: Complex = 4 + 3.i
*  }}}
*/
object Test {

	/**
	 * Metodo principale dell'oggetto. Questo è un esempi: Factory for [[it.au.misure.ingestione.ImportProcessFiltroValidi]] instances.
	 * 
	 *  ==Overview==
	 *  prova microfono prova.
	 * 
	 *  <pre><span class="kw">
	 *  import ComplexImplicits._ fattoo di si riprende mai.
	 *  </span></pre> 
	 *  
	 *  
	 *  Questa è la funzione che viene richiamata quando si esegue la validazione del formato XSD dei file di misura.
	 *  
	 *  ==Overview==
	 *  {{{
	*  val rddXsd = rddValidAll.map(validationXSD( _ , defdomplextypes,defsimpletypes,flusso1_pdo)).setName("Validazione xsd")
	*  }}}
	* 
	* Questo è un esempio di porzione di codice nella documentazione.
	* 
	* 
	* @param Array[String] sono parametri di input di default ma non vengono utilizzati, si può ancahe instaziare a null.
	* @return questo metodo non prevede risultati in output.
	*/
	def main(args: Array[String]) {
		test6(args)
	}
	
	@transient lazy val log = org.apache.log4j.LogManager.getRootLogger
	
	def test6(args: Array[String]) = {
	  val s = "_R,_NR".split(",")
	  val in:String = "01234567890_12345678901_201301_PDO2G_20130218060523_1DP0001_R" 
	    
	  println( isNuovo2G("_R,_NR", in) )
	}
	
	def isNuovo2G(regex:String, in:String) : Boolean = {
	  val s = regex.split(",")
	  var ret:Boolean = false
	  for(e <- s) if(in.contains(e)){
	    ret=true
	  }
	  ret
	}
	
	def test5(args: Array[String]) = {
	  val in:Double = 2.9808

	  val n = BigDecimal( in ).setScale(0, BigDecimal.RoundingMode.HALF_UP)
	  
	  println( n )
	}
	
	def test4(args: Array[String]) = {
	  val f = new File("file:/au2g/AU/shared_dir/TMP_1G/misure_1G/TME_05779711000/DISTRIBUTORE/TME_05779711000_12874490159/2018/0401/05779711000_12874499159_201803_PDO_20180401174527_1DP0073.xml")
	  val s = f.getName()
	  println(s)
	}
	
	def eaValues(args: Array[String]) = {
	  	  val eaValues = (1 to 100).map { q =>
	    "\"e"+q+"\""
	  }.toList.mkString(",")
	  
	  println(eaValues)
	}

	def test3(args: Array[String]) = {
      log.setLevel(Level.WARN)

      val envVar = s"/user/${System.getProperty("user.name")}/deploy"

      val commandLineOptions = new CommandLineOptions()
			val commandLine:CommandLine = new CommonsCliUtils().parseArgsList(args, commandLineOptions.getOptions)
					val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)

					println("***** Inizio processo " + argsObjMaster.appName + " *****")

					val conf = new SparkConf()
					.setAppName( argsObjMaster.appName )
					.set("spark.shuffle.service.enabled", "false")
					.set("spark.dynamicAllocation.enabled", "false")
					.set("spark.io.compression.codec", "snappy")
					.set("spark.rdd.compress", "true")
					.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
					.setMaster( argsObjMaster.master )
					//			.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
					//			.set("spark.kryoserializer.buffer", "1m")
					//			.set("spark.kryoserializer.buffer.max", "512m")
					//			.registerKryoClasses(Array(classOf[it.au.misure.ImportProcessV8]))
					//			 .setMaster("yarn-client")

					val sc = new SparkContext(conf)
					sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
					sc.setLogLevel(argsObjMaster.logLevel)

					try{
						val sqlCtx = new SQLContext(sc)
								sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
								sqlCtx.setConf("spark.sql.parquet.compression.codec","uncompressed")
								sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
								sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
								sqlCtx.setConf("hive.exec.dynamic.partition","true")
								sqlCtx.setConf("hive.exec.dynamic.partition.mode","nonstrict")

								val minPartitions =  sc.getConf.get("spark.fm.min.partitions").toInt
								val quartiPDO = sc.getConf.get("spark.fm.quarti")
								val report =  sc.getConf.get("spark.fm.report")


								val pdo_rfo:String = argsObjMaster.PdoRfo
								val isAggiorna:Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)
								val injectionTmp:String = argsObjMaster.injectionTmp
								val rootDir:String = argsObjMaster.rootDir
								val slash =  sc.getConf.get("spark.fm.slash")
								val anno:String = argsObjMaster.anno
								val mese:String = argsObjMaster.mese
								val giorno:String = argsObjMaster.giorno
								//XSD
								val defdomplextypes =  sc.getConf.get("spark.validazione.xsd.defdomplextypes")
								val defsimpletypes =  sc.getConf.get("spark.validazione.xsd.defsimpletypes")
								val flusso1_pdo =  sc.getConf.get("spark.validazione.xsd.flusso1_pdo")
								val flusso1_rfo =  sc.getConf.get("spark.validazione.xsd.flusso1_rfo")
								val flusso1_pdo_v2 =  sc.getConf.get("spark.validazione.xsd.flusso1_pdo_v2")
								val flusso1_rfo_v2 =  sc.getConf.get("spark.validazione.xsd.flusso1_rfo_v2")

								/* Perdita di tensione */
								val prop:Properties = new CreateProperties().prop
								val perdita_380 = prop.getProperty("spark.app.perdita.tensione.380").toDouble
								val perdita_220 = prop.getProperty("spark.app.perdita.tensione.220").toDouble
								val perdita_150 = prop.getProperty("spark.app.perdita.tensione.150").toDouble
								val perdita_1_35 = prop.getProperty("spark.app.perdita.tensione.1_35").toDouble
								val perdita_1 = prop.getProperty("spark.app.perdita.tensione.1").toDouble
								val v2_2G:String = prop.getProperty("spark.app.v2_2G")

								val dataelaborazione = new java.sql.Timestamp( System.currentTimeMillis() )

								val injection1G:String = sc.getConf.get("spark.fm.injection1G")
								val injection2G:String = sc.getConf.get("spark.fm.injection2G")


								println("*** sc.master: " + sc.master)
								println("*** injection1G: " + injection1G)
								println("*** injection2G: " + injection2G)
								println("*** injectionTmp: " + injectionTmp)
								println("*** rootDir: " + rootDir)
								println("*** minPartitions: " + minPartitions)
								println("*** slash: " + slash)
								println("*** envVar: " + envVar)
								//hdfs
								println("*** quartiPDO: " + quartiPDO)
								println("*** report: " + report)
								println("*** pdo_rfo: " + pdo_rfo)
								println("*** isAggiorna: " + isAggiorna)

								//XSD
								println("*** defdomplextypes: " + defdomplextypes)
								println("*** defsimpletypes: " + defsimpletypes)
								println("*** flusso1_pdo: " + flusso1_pdo)
								println("*** flusso1_rfo: " + flusso1_rfo)
								println("*** flusso1_pdo_v2: " + flusso1_pdo_v2)
								println("*** flusso1_rfo_v2: " + flusso1_rfo_v2)
								println("*** dataelaborazione: " + dataelaborazione)

								//perdita di tensione
								println("*** perdita_380: " + perdita_380)
								println("*** perdita_220: " + perdita_220)
								println("*** perdita_150: " + perdita_150)
								println("*** perdita_1_35: " + perdita_1_35)
								println("*** perdita_1: " + perdita_1)

								val isGiornoSingolo:Boolean = if(commandLine.hasOption(commandLineOptions.giorno.getOpt)) true else false
//								val giorniList:List[String] = if (isGiornoSingolo)  List(giorno) else CommonsCliUtils.getGiorni(commandLine) 

					}catch{
					case ex: FileNotFoundException => ex.printStackTrace()
					case e: Exception =>  e.printStackTrace()
					}finally{
						sc.stop()
					}
	}




	def test2() = {
			val flaguddpodVal:Boolean = true
					val trattamentoVal:Boolean = false
					val validatoVal:Boolean = true

					val flagValidazioni = if(flaguddpodVal && trattamentoVal && validatoVal) "Y" else  s"${if(!flaguddpodVal) "F"  else ""}${if(!trattamentoVal) "T" else ""}${if(!validatoVal) "V"  else ""}"


					println( flagValidazioni )
	}

	def test() = {
			val anno = "2016"
					val mese = "02"

					val sdf = new SimpleDateFormat("ddMMyyyy");
			val date = sdf.parse("01" + mese + anno);

			val gc = new GregorianCalendar()
					gc.setTime(date)

					println("gc " + gc.getActualMaximum(Calendar.DAY_OF_MONTH))

					val timestamp = new Date().getTime()

					println(timestamp)

					println("formattazione misure")
					val t = 1830.3480000000002D

					println(scalaCustom(t))

					import scala.collection.JavaConversions._

					val environmentVars = System.getenv()
					for ((k,v) <- environmentVars) println(s"key: $k, value: $v")

					val properties = System.getProperties()
					for ((k,v) <- properties) println(s"key: $k, value: $v")

					println("spark.jdbc.rcu.driver: " + System.getProperty("spark.jdbc.rcu.driver"))


					val s = "11DP0426.xml"
					val CodContrDispNF = "DP0426.xml".split("\\.")(0)
					val l = CodContrDispNF.length() - 6
					val c = CodContrDispNF.substring(l)
					println(c)
	}

	def scalaCustom(x: Double) = {
			val roundBy = 0
					val w = math.pow(10, roundBy)
					(x * w).toLong.toDouble / w
	}

}