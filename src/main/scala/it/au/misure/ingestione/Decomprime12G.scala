package it.au.misure.ingestione

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import scala.util.control.Breaks._
import collection.mutable.ArrayBuffer
import java.io.File
import java.io.FilenameFilter
import it.au.misure.commons.cli.{CommandLine}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
import it.au.misure.util.LoggingSupport
import it.au.misure.util.ZipArchive
import org.apache.spark.storage.StorageLevel
import org.apache.spark._
import org.apache.spark.sql
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row

/**
 * ==FM Decompressione==
 * Decomprime gli archivi 1G/2G su una tabella temporanea. I flussi misura vengo acquisiti in formato zip sotto un’alberatura di sottocartelle predefinita, 
 * quindi la prima fase è la lettura di tutti i file dell’alberatura che devono essere acquisiti.
 * Nella seconda fase i file vengono decompressi in una cartella temporanea mantenendo la stessa alberatura originale per essere successivamente acquisiti dal processo stesso.
 * 
 * 
 */
object Decomprime12G extends LoggingSupport {
  
/**
 * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
 * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
 */
	def main(args: Array[String]) {
	 
		val commonsCliUtils = new CommonsCliUtils()
		val commandLineOptions = new CommandLineOptions()
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
		val argsObjMaster = commonsCliUtils.getArgs(commandLine)
		
		println("***** Inizio processo Decompressione Misure Quarti *****")

		val conf = new SparkConf()
  		.setAppName( argsObjMaster.appName )
  		.set("spark.shuffle.service.enabled", "false")
  		.set("spark.dynamicAllocation.enabled", "false")
  		.setMaster( argsObjMaster.master )

		val sc = new SparkContext(conf)
		sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
		sc.setLogLevel(argsObjMaster.logLevel)
		
		val minPartitions =  sc.getConf.get("spark.fm.min.partitions")
		
		val sqlCtx = new SQLContext(sc)
  	sqlCtx.setConf("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version","2")
  	sqlCtx.setConf("spark.sql.parquet.compression.codec","uncompressed")
  	sqlCtx.setConf("spark.sql.parquet.binaryAsString", "true")
  	sqlCtx.setConf("spark.sql.parquet.output.committer.class", "org.apache.spark.sql.parquet.ParquetOutputCommitter")
  	sqlCtx.setConf("hive.exec.dynamic.partition","true")
  	sqlCtx.setConf("hive.exec.dynamic.partition.mode","nonstrict")
		
		try{

    		val slash = sc.getConf.get("spark.fm.slash")
    		val minPartitions =  sc.getConf.get("spark.fm.min.partitions").toInt
    		val injectionTmp:String = argsObjMaster.injectionTmp
    		val rootDir:String = argsObjMaster.rootDir

				val injection:String = if (commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
					sc.getConf.get("spark.fm.injection1G").concat(rootDir).concat(File.separator)
				}else if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
					sc.getConf.get("spark.fm.injection2G").concat(rootDir).concat(File.separator)
				}else{
					commonsCliUtils.printHelpForOptions(commandLineOptions.getOptions)
					System.exit(0)
					""
				}
		
				val isAggiorna:Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)
				val pdo_rfo:String = argsObjMaster.PdoRfo
				val isGiornoSingolo:Boolean = if(commandLine.hasOption(commandLineOptions.giorno.getOpt)){
					true
				}else{
					false
				}

				println("*** sc.master: " + sc.master)
				println("*** sc.sparkUser: " + sc.sparkUser)
				println("*** injection: " + injection)
				println("*** injectionTmp: " + injectionTmp)
				println("*** rootDir: " + rootDir)
				println("*** minPartitions: " + minPartitions)
				println("*** pdo_rfo: " + pdo_rfo)
				println("*** isAggiorna: " + isAggiorna)


				val giorniList:List[String] = if (isGiornoSingolo) {
					List("giorno_singolo")
				}else{
					commonsCliUtils.getGiorni(commandLine,commandLineOptions)
				}
				

				val tmpDirClean = new File(s"${injectionTmp}${File.separator}${rootDir}")
				
				// /mnt/isilonshare1/TMP_1G_collaudo0720/Test_clouderaShare
				if(tmpDirClean.exists() && tmpDirClean.isDirectory() && tmpDirClean.listFiles().length > 0){
				  val tmpDirList = tmpDirClean.listFiles().map(xml => xml.getPath).toList
				  val oldFiles = sc.parallelize(tmpDirList, 20).setName("Scansiona alberatura temporanea")
					oldFiles.map(xmlDir => delete(new File(xmlDir)) ).setName("Cancella alberatura temporanea").collect()
					tmpDirClean.delete()
					println("*** pulizia della cartella temporanea prima della scompattazione OK")
				}
				println("*** controllo della la cartella temporanea OK")

				
				val elencoUDD = scansionaUDD(injection).toSeq
					
				val rddUDD = sc.parallelize(elencoUDD).setName("Scansiona alberatura")
				rddUDD.cache()
				println("*** scansiona alberatura UDD OK ")
					
				rddUDD.foreach{uddDir => 
  				val ret = leggiAlbertatura(uddDir, giorniList, args, commonsCliUtils, commandLineOptions)
  				ret.foreach(filePath => decomprimiAlberatura(filePath, rootDir, injectionTmp))
				}
					
				println("*** decomprimi alberatura OK")
		}catch{
  		case e: Exception =>  {
  			log.error(e.getMessage, e)
  			throw new Exception(e)
  		}
		}finally{
			sc.stop()
		}

		println("***** Fine processo Decompressione Misure Quarti *****")
	}
	
	def pri(udd:String):String = {
	  println(s"udd info => ${udd}")
	  udd
	}
	
/**
 * Funzione iterativa che elimina i file nella cartella temporanea.
 * @param f path del file da cancellare, se è una directory individua i file xml in essa contenuti, se è un file concella.
 */
	def delete(f:File):Boolean = {
    if (f.isDirectory()) {
      for (c <- f.listFiles())
        delete(c);
    }
    if (!f.delete()){
      false
    }else{
      true
    }
}
	
	def scansionaTmpDirPath(tmpDirPath:String, slash:String) : List[String] = {
	  try{
	    
  	  val xmls = new File ( tmpDirPath )
  		val xmlPathList = if(xmls.exists() && xmls.isDirectory() && xmls.listFiles().length > 0){
	    val xmlList = xmls.listFiles().map{ xml => 
	         val path = "file://" + slash +  xml.getPath
 		       path
	       }
	       xmlList.toList
	     }else{
	       List( )
	     }
			xmlPathList
		} catch {
				case e: Exception => {
					e.printStackTrace()
					throw new Exception(e)
				}
	  }
	}
	
	def ottieniAlberatura(file:String, rootDir:String, injectionTmp:String) : String = {
	  val dirPath = file.substring(file.lastIndexOf(rootDir), file.lastIndexOf(File.separator))
	  val tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)
	  tmpDirPath
	}
	
/**
 * Decomprime i file compressi nella cartella temporanea.
 * @param uddDir cartella del distributore.
 * @param file nome del file compresso da decomprimere.
 * @param rootDir cartella root che contiene i file compressi.
 * @param injectionTmp cartella temporanea di destinazione dei file di misura decompressi.
 */
	def decomprimiAlberatura(file:String, rootDir:String, injectionTmp:String) : String = {
			try{
				val zipArchive = new ZipArchive()
				val dirPath = file.substring(file.lastIndexOf(rootDir), file.lastIndexOf(File.separator))
				val tmpDirPath = injectionTmp.concat(File.separator).concat(dirPath)
				val tmpDir = new File(tmpDirPath)
				tmpDir.mkdirs()
				zipArchive.unZip(file, tmpDirPath)

				tmpDirPath
			} catch {
  			  case e: Exception => {
    				e.printStackTrace()
    				throw new Exception(e)
  			  }
			}
	}
	
/**
 * Scansiona gli archivi compressi nelle cartelle dei distributori in base all'anno, mese, giorno specificati.
 * @param uddDir cartella del distributore.
 * @param args argomenti passati da linea di comando al processo di decompressione.
 * @param commonsCliUtils oggetto di utilità per la gestione degli argomenti da linea di comando.
 * @param commandLineOptions oggetto di utilità per la gestione degli argomenti da linea di comando.
 * @return elenco archivi compressi.
 */
	def leggiAlbertatura(uddDir:String, giorniList:List[String], args: Array[String], commonsCliUtils:CommonsCliUtils, commandLineOptions:CommandLineOptions) : List[String] = {
	  try{
  	  val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
  		val argsObjMaster = commonsCliUtils.getArgs(commandLine)
  	  val isAggiorna:Boolean = commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)
  		val pdo_rfo:String = argsObjMaster.PdoRfo
  		val isGiornoSingolo:Boolean = if(commandLine.hasOption(commandLineOptions.giorno.getOpt)){
  			true
  		}else{
  			false
  		}
  	  
  	  val ret = giorniList.map{ giornoX => 

						/* singolo giorno */
						val argsObj = if(isGiornoSingolo){
							argsObjMaster
						}else{
							commonsCliUtils.getArgsRange(commandLine,commandLineOptions, giornoX)
						}

						val anno:String = argsObj.anno
						val mese:String = argsObj.mese 
						val giorno:String = argsObj.giorno
						val nomeFile:String = argsObj.nomeFile
						val nomeFile2G:String = argsObj.nomeFile2G
						val xmlDirPath = uddDir + File.separator + anno + File.separator + mese + giorno

						val annoMeseGiornoDir = anno + mese + giorno
						val xmlDir = new File( xmlDirPath )
						/* scansiona i file di ogni udd */
						val abW3 = if(xmlDir.exists() && xmlDir.isDirectory() && xmlDir.listFiles().length > 0){
							val xmlFiles = xmlDir.listFiles(new FilenameFilter {
								override def accept(dir: File, name: String): Boolean = {

										val data:String = {
												val o = name.split("_")
														if(o.length > 2){
															o(2)
														} else {
															"NESSUNA DATA"
														}
    								}
    
    								
    								val ret:Boolean = if(pdo_rfo.equals("RFO")) {
    								  ( name.contains("_RFO_") || name.contains("_RFO2G_") ) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
    								}else{
    								  ( name.contains("_PDO_") || name.contains("_PDO2G_") ) && ( name.endsWith(".zip") || name.endsWith(".ZIP") )
    								}
    								
    								val rr = if(ret)  {
    								  val containsName2G = name.contains("_2G")
    								  val nameSplit = name.split("_")(2).toInt
    								  val nomeFile2GInt = nomeFile2G.toInt
    								  val nomeFileInt = nomeFile.toInt
    								  val r = if(containsName2G){
    								    val a = if(nameSplit <= nomeFile2GInt) {
    								      true
    								      }else{ 
    								        false
    								      }
    								    a
    								  }else{
    								    val b = if(nameSplit <= nomeFileInt){ 
    								      true
    								      } else{ 
    								        false
    								     }
    								    b
    								  }
    								  r
    								}else {
    								  ret
    								}
    								rr
								}
							})

							val ret = xmlFiles.toList
							ret
						} else {
						  List( )
						}
						abW3
    	    }
      	  val retList = ret.flatMap(f => f).map(_.getPath)
      	  retList
  	  	} catch {
  				 case e: Exception => {
  					 e.printStackTrace()
  					 throw new Exception(e)
  			}
  	}
	}
	
/**
 * Scansiona le cartelle dei distributori.
 * @param injectionPath path delle cartelle dei distributori.
 * @return elenco archivi compressi.
 */
		def scansionaUDD(injectionPath:String) : List[String] = {
			val abW2 = new ArrayBuffer[(String)]()

					//cartella principale 
					for (xmlPrincipaleDir <- new File(injectionPath).listFiles()) {
						//				println("XXX xmlPrincipaleDir: " + xmlPrincipaleDir.getPath)
						if(xmlPrincipaleDir.exists() && xmlPrincipaleDir.isDirectory() && xmlPrincipaleDir.listFiles().length > 0){
							//cartella del distributore
							val pivaDistrDir = xmlPrincipaleDir.getPath()

							try{
									for (xmlUddDir <- new File(pivaDistrDir + File.separator + "DISTRIBUTORE").listFiles()) {
										if( xmlUddDir.exists() && xmlUddDir.isDirectory() &&  xmlUddDir.listFiles().length > 0){
											//cartella del sotteso
											val uddDir = xmlUddDir.getPath()

											abW2.+=(uddDir)
										}
									}
							
							}catch{
          		case e: Exception =>  {
          		  e.printStackTrace()
          		  throw new Exception(e)
          		}
          	}
							
						}
					}
			abW2.toList
	}


	//in caso di aggiornamento considero solo i distributori_utenti specificati se ci sono
	def distrUteList(commandLine: CommandLine,commandLineOptions:CommandLineOptions, nomeFile:String) : Boolean = {
			if(commandLine.hasOption(commandLineOptions.distrUteAgg.getOpt)){
				val dul = commandLine.getOptionValue(commandLineOptions.distrUteAgg.getOpt).split(',')
						var ret:Boolean = false

						dul.foreach{f =>
						if(nomeFile.contains(f)){
							ret = true
						}
				}
				ret
			}else{
				true
			}
	}

}