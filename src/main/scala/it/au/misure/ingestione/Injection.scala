package it.au.misure.ingestione

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
import java.util.ArrayList
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
import it.au.misure.util.CreateProperties
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.cli.{CommonsCliUtils, CommandLineOptions}
//import it.au.misure.cli.CommonsCliUtils.Args
import org.apache.log4j.Logger
import it.au.misure.util.ZipArchive
import it.au.misure.util.LoggingSupport

//import org.apache.commons.io.input.BOMInputSteam 
//import scala.io.Source
import java.io.FileInputStream
import scala.xml.pull.XMLEventReader

/**
 * ==FM Inserimento Misure Quarti==
 * E' il processo di ingesione degli xml dal repository. Acquisisce i file di misura ed esegue alcune validazioni assegnando ad ogni record
 * un flag. Le validazioni si riferiscono alla congruenza della coppia Udd-Pod, alla determinazione dello stato e del tipo di trattamento e alla congruenza con l'area di appartenenza.
 * Il processo inserisce le informazioni ricavate dalla lettura dei file xml e dalle validazioni delle informazioni in essi
 * contenuti nella tabella hdfs denominata ''fm_quarti''.
 * 
 * 
 * @see [[https://spark.apache.org/docs/latest/job-scheduling.html]]
 * 
 * 
 */
object Injection extends LoggingSupport {

      val format = new SimpleDateFormat("yyyy-MM-dd")
      val UTF8_BOM =  "\uFEFF"
      
    	/**
    	 * Legge le variabili del file di properties.
    	 */
    	val prop:Properties = new CreateProperties().prop

			/**
			 * Perdita di tensione
			 */
			 val perdita_380 = prop.getProperty("spark.app.perdita.tensione.380").toDouble
			 val perdita_220 = prop.getProperty("spark.app.perdita.tensione.220").toDouble
			 val perdita_150 = prop.getProperty("spark.app.perdita.tensione.150").toDouble
			 val perdita_1_35 = prop.getProperty("spark.app.perdita.tensione.1_35").toDouble
			 val perdita_1 = prop.getProperty("spark.app.perdita.tensione.1").toDouble
			 val v2_2G:String = prop.getProperty("spark.app.v2_2G")
			 val regex_2G:String = prop.getProperty("spark.app.regex_2G")

			 /**
			  * Definizione files XSD
			  */
			 val xsdProp:Properties = new CreateProperties().xsdProp
			 
			 val defdomplextypes:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes")
       val defsimpletypes:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes")
       val flusso1_pdo:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo")
       val flusso1_rfo:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo")
       val flusso1_pdo_v2:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo_v2")
       val flusso1_rfo_v2:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo_v2")
       
       val defdomplextypes_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes.straniere")
       val defsimpletypes_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes.straniere")
       val flusso1_pdo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo.straniere")
       val flusso1_rfo_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo.straniere")
       val flusso1_pdo_v2_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_pdo_v2.straniere")
       val flusso1_rfo_v2_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso1_rfo_v2.straniere")
       
       val defdomplextypes_2G:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes_2G")
       val defsimpletypes_2G:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes_2G")
       val flusso_pdo_2G:String = xsdProp.getProperty("spark.validazione.xsd.flusso_pdo_2G")
       val flusso_rfo_2G:String = xsdProp.getProperty("spark.validazione.xsd.flusso_rfo_2G")
       
       val defdomplextypes_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defdomplextypes_2G.straniere")
       val defsimpletypes_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.defsimpletypes_2G.straniere")
       val flusso_pdo_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso_pdo_2G.straniere")
       val flusso_rfo_2G_straniere:String = xsdProp.getProperty("spark.validazione.xsd.flusso_rfo_2G.straniere")

			/**
			 * Esegue il parsing dei file xml ed esegue le validazioni richiamando le funzioni ''validazioneOrariePodUdd'' e ''validazioneStatoPod''.
			 * @param rdd1 rappresenta il file xml
			 * @return una lista di record contenti le informazioni del file xml
			 */
       def letturaEValidazioni_vecchioFormato(rdd1: (Boolean,(String,String),String), dataelaborazione:java.sql.Timestamp): List[(Row)] = {
          try {
				  
  				    val noBom = if(rdd1._2._2.startsWith(UTF8_BOM)){
  						  rdd1._2._2.substring(1)
  						}else{
  						  rdd1._2._2
  						}
					    val xml = XML.loadString(noBom)
							val datiPod = (xml \\ "FM" \\ "DatiPod")

							val pivadistributore = (xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text
							val pivautente = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
							val codContrDisp = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text

							val dpIt = datiPod.theSeq

							
							val dpRes = dpIt.flatMap { dp =>

    							val pod = (dp \\ "Pod").text
    							val nomefile = rdd1._2._1
    							val nomefileT =  new File( nomefile ).getName().toUpperCase()
    							val sp = nomefile.split("/")
    							val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
    
    							val timeStamp:Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong
    
    							val meseAnno = ((dp \\ "MeseAnno").text).split("/")
    							val area = (dp \\ "DatiPdp" \\ "PuntoDispacciamento").text
    							val tensioneVista = (dp \\ "DatiPdp" \ "Tensione").text.toDouble 
    							val tensione = tensioneVista / 1000
    							val trattamento_o = (dp \\ "DatiPdp" \\ "Trattamento").text
    							val potcontrimpl = Try((dp \\ "DatiPdp" \ "PotContrImp").text.replace(",", ".").toDouble).getOrElse(0D)
    							val potdisp = Try((dp \\ "DatiPdp" \ "PotDisp").text.replace(",", ".").toDouble).getOrElse(0D)
    							val cifreatt = Try(((dp \\ "DatiPdp" \ "CifreAtt")text).toInt).getOrElse(0)
    							val cifrerea = Try(((dp \\ "DatiPdp" \ "CifreRea")text).toInt).getOrElse(0)
							

									val tipodato = (dp \\ "Curva" \ "TipoDato").text
									val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
							    val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }

							    val raccolta = (dp \\ "Curva" \ "Raccolta").text
									val validatoTmp = (dp \\ "Curva" \ "Validato").text//).getOrElse("S")
									val potmax = (dp \\ "Curva" \ "PotMax").text.replace(",", ".").toDouble
									
									val validato = if(validatoTmp.trim().equals("")){
									  "S"
									}else {
									  validatoTmp
									}
							    

									// <Tensione>132000</Tensione> / 1000 = V
									val perditatens = if(tensione <= 1){
										perdita_1 // 0.104 // 10.4%
									}else if(tensione > 1 && tensione <= 35){
										perdita_1_35 // 0.04 // 4%
									}else if(tensione <= 150){
										perdita_150 // 0.018 // 1.8%
									}else if(tensione == 220){
										perdita_220 // 0.011 // 1.1
									}else if(tensione > 220){
										perdita_380 // 0.007
									}else {
										tensione
									}

							     
    							val ea = (dp \\ "Curva" \ "Ea")
									val it = ea.theSeq

									
									val anno = meseAnno(1)
    							val mese = meseAnno(0)
    							
									//passaggio ora solare/legale (attività che si esegue solo a ottobre)
    							val oraLegale = if(mese.toInt == 10){
    							  val itResDst =  it.map { e =>
  									  val t = (e \ (s"@Dst")text)
  									  
  									  val tt = if(t.equals("2")){
      									
  									    val e1_12_dist2 = (1 to 12).map { q =>
      											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)
      											el.getOrElse(0D)
      									}.toList
      									
      									e1_12_dist2
  									  }else{
  									    Nil
  									  }
  									  
  									  (t,tt)
  									}.filter(f=> f._1.equals("2"))
  									
  									val ret = if(itResDst.size > 0){
  									  itResDst(0)._2
  									}else{
  									  Nil
  									}
  									
  									ret
    							}else{
    							  Nil
    							}

							    val coduc = "UC_" + codContrDisp + "_" + area
							    
							    val commonFields = List(codContrDisp, coduc, pod, pivautente, pivadistributore, area, anno.toInt, mese.toInt,  //dataTime, 
    													tipodatoE,tipodatoS,tensioneVista,trattamento_o,potcontrimpl,potdisp,cifreatt,cifrerea,raccolta,validato,potmax,perditatens,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp)
    
    							//passaggio ora solare/legale (attività che si esegue solo a ottobre)
    							val itRes = if(mese.toInt == 10){
    							   val itResLeg =  it.map { e =>
    									val giorno = e.text
    									
    									val t = (e \ (s"@Dst")text)
  									  val eaValues = if(t.equals("2")){
  									    (false, Nil)
  									  }else if(t.equals("3")){
    									  
    									  val e9_12_dist3 = (9 to 12).map { q =>
    											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)
    											el.getOrElse(0D) 
    									  } toList
    									  
    									  val e13_96_dist3 = (13 to 96).map { q =>
    											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)
    											el.getOrElse(0D) 
    									  } toList
    									  
    									  val e1_12_dist2 = oraLegale
    									  
    									  (true, e1_12_dist2 ++ e13_96_dist3 ++ e9_12_dist3)
  									  }else{
  									    val ret = (1 to 100).map { q =>
    											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)
    											el.getOrElse(0D) 
    									  } toList
    									  
    									  (true, ret)
  									  }
    									
    
    									(eaValues._1, commonFields ++ List(giorno.toInt) ++ eaValues._2)
    							  }.filter(f => f._1).map( _._2 )
    							
    							  itResLeg
    							}else{
    							  val itResSol =  it.map { e =>
    									val giorno = e.text
    									val eaValues = (1 to 100).map { q =>
    											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)
    											el.getOrElse(0D) 
    									} toList
    
    									(commonFields ++ List(giorno.toInt) ++ eaValues)
    							  }
    							  
    							  itResSol
    							}
    													

      					//misure stimate
      					val er = (dp \\ "Curva" \ "Er")
  							val ir = er.theSeq
  
  							val irRes =  ir.map { e =>
  									val erValues = (1 to 100).map { q =>
  
  									val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble)//.getOrElse(0D)
  									el.getOrElse(0D) //* (1 + perditatens)
  
  									} toList

  									erValues
  							}
  
  							val curva = for ( (a, r) <- (itRes zip irRes)) yield  Row.fromSeq(a ++ r)// ++ newFields)
  									curva
  					}
  
  					dpRes.toList

				} catch {
  				case e: Exception => {
  					e.printStackTrace()
  					Nil
  				}
				}
      }
  
       def letturaEValidazioni_2G(rdd1: (Boolean,(String,String),String), dataelaborazione:java.sql.Timestamp): List[(Row)] = {
          try {
				  
  				    val noBom = if(rdd1._2._2.startsWith(UTF8_BOM)){
  						  rdd1._2._2.substring(1)
  						}else{
  						  rdd1._2._2
  						}
					    val xml = XML.loadString(noBom)
							val datiPod = (xml \\ "FM" \\ "DatiPod")

							val pivadistributore = (xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text
							val pivautente = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
							val codContrDisp = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text

							val dpIt = datiPod.theSeq

							val dpRes = dpIt.flatMap { dp =>

    							val pod = (dp \\ "Pod").text
    							val nomefile = rdd1._2._1
    							val nomefileT =  new File( nomefile ).getName().toUpperCase()
    							val sp = nomefile.split("/")
    							val annoMeseGiornoDir = (s"${sp(sp.length - 3)}${sp(sp.length - 2)}").toInt
    
    							val timeStamp:Long = nomefile.substring(nomefile.lastIndexOf("/")).split("_")(4).toLong
    
    							val meseAnno = ((dp \\ "MeseAnno").text).split("/")
//    							val area = (dp \\ "DatiPdp" \\ "PuntoDispacciamento").text
    							val tensioneVista = (dp \\ "DatiPdp" \ "Tensione").text.toDouble 
    							val tensione = tensioneVista / 1000
    							val trattamento_o = (dp \\ "DatiPdp" \\ "Trattamento").text
    							

									val tipodato = (dp \\ "Misura" \ "TipoDato").text
									val tipodatoE = if(tipodato == "E"){ 1 } else { 0 }
							    val tipodatoS = if(tipodato == "S"){ 1 } else { 0 }

							    val raccolta = (dp \\ "Misura" \ "Raccolta").text
									val validatoTmp = (dp \\ "Misura" \ "Validato").text//).getOrElse("S")
									val potmax = (dp \\ "Misura" \ "PotMax").text.replace(",", ".").toDouble
									
									val validato = if(validatoTmp.trim().equals("")){
									  "S"
									}else {
									  validatoTmp
									}
							    
							    log.info(validato)

									// <Tensione>132000</Tensione> / 1000 = V
									val perditatens = if(tensione <= 1){
										perdita_1 // 0.104 // 10.4%
									}else if(tensione > 1 && tensione <= 35){
										perdita_1_35 // 0.04 // 4%
									}else if(tensione <= 150){
										perdita_150 // 0.018 // 1.8%
									}else if(tensione == 220){
										perdita_220 // 0.011 // 1.1
									}else if(tensione > 220){
										perdita_380 // 0.007
									}else {
										tensione
									}
							    
							    
							    	// nuovi campi
							    val forfait = (dp \\ "DatiPdp" \\ "Forfait").text
    							val gruppomis = (dp \\ "DatiPdp" \ "GruppoMis").text
    							val ka = Try(((dp \\ "DatiPdp" \ "Ka")text).toDouble).getOrElse(1D)
    							val kr = Try(((dp \\ "DatiPdp" \ "Kr")text).toDouble).getOrElse(1D)
    							
    							val eaf1 = (dp \\ "Misura" \ "EaF1").text.replace(",", ".").toDouble
    							val eaf2 = (dp \\ "Misura" \ "EaF2").text.replace(",", ".").toDouble
    							val eaf3 = (dp \\ "Misura" \ "EaF3").text.replace(",", ".").toDouble
    							val eaf4 = (dp \\ "Misura" \ "EaF4").text.replace(",", ".").toDouble
    							val eaf5 = (dp \\ "Misura" \ "EaF5").text.replace(",", ".").toDouble
    							val eaf6 = (dp \\ "Misura" \ "EaF6").text.replace(",", ".").toDouble
    							
    							val erf1 = (dp \\ "Misura" \ "ErF1").text.replace(",", ".").toDouble
    							val erf2 = (dp \\ "Misura" \ "ErF2").text.replace(",", ".").toDouble
    							val erf3 = (dp \\ "Misura" \ "ErF3").text.replace(",", ".").toDouble
    							val erf4 = (dp \\ "Misura" \ "ErF4").text.replace(",", ".").toDouble
    							val erf5 = (dp \\ "Misura" \ "ErF5").text.replace(",", ".").toDouble
    							val erf6 = (dp \\ "Misura" \ "ErF6").text.replace(",", ".").toDouble
    							
    							val potf1 = (dp \\ "Misura" \ "PotF1").text.replace(",", ".").toDouble
    							val potf2 = (dp \\ "Misura" \ "PotF2").text.replace(",", ".").toDouble
    							val potf3 = (dp \\ "Misura" \ "PotF3").text.replace(",", ".").toDouble
    							val potf4 = (dp \\ "Misura" \ "PotF4").text.replace(",", ".").toDouble
    							val potf5 = (dp \\ "Misura" \ "PotF5").text.replace(",", ".").toDouble
    							val potf6 = (dp \\ "Misura" \ "PotF6").text.replace(",", ".").toDouble
    							
    							val newFields = List(forfait,gruppomis,ka,kr,eaf1,eaf2,eaf3,eaf4,eaf5,eaf6,erf1,erf2,erf3,erf4,erf5,erf6,potf1,potf2,potf3,potf4,potf5,potf6)
							    

    							val ea = (dp \\ "Misura" \ "Ea")
									val it = ea.theSeq

									val anno = meseAnno(1)
    							val mese = meseAnno(0)
									
									val itRes =  it.map { e =>

    									val giorno = e.text
    									
    									val eaValues = (1 to 100).map { q =>
    
    											//s -> The simple string interpolator!!
    											val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble * ka)//.getOrElse(0D)
    											el.getOrElse(0D) //* (1 + perditatens)
    
    									} toList
    									
    
                        val commonFields = List(codContrDisp, "", pod, pivautente, pivadistributore, "", anno.toInt, mese.toInt, giorno.toInt, //dataTime, 
    													tipodatoE,tipodatoS,tensioneVista,trattamento_o,"","","","",raccolta,validato,potmax,perditatens,nomefileT,annoMeseGiornoDir,dataelaborazione,timeStamp)
    
    											(commonFields ++ eaValues ++ newFields)
    							}

      							//misure stimate
      							val er = (dp \\ "Curva" \ "Er")
  									val ir = er.theSeq
  
  									val irRes =  ir.map { e =>
  									val erValues = (1 to 100).map { q =>
  
  									val el = Try( (e \ (s"@E$q")text).replace(",", ".").toDouble * kr)//.getOrElse(0D)
  									el.getOrElse(0D) //* (1 + perditatens)
  
  									} toList

  									erValues
  							}
  
  							val curva = for ( (a, r) <- (itRes zip irRes)) yield  Row.fromSeq(a ++ r)
  									curva
  					}
  
  					dpRes.toList

				} catch {
  				  case e: Exception => {
  					  e.printStackTrace()
  					  Nil
  				}
				}
      }
       
  	def letturaEValidazioni(rdd1: (Boolean,(String,String),String), dataelaborazione:java.sql.Timestamp): List[(Row)] = {
  		val nomeFile = rdd1._2._1
  	
    	val ret = if(isNuovo2G(regex_2G, nomeFile)){
    	  letturaEValidazioni_2G(rdd1, dataelaborazione)
    	}else{
    	  letturaEValidazioni_vecchioFormato(rdd1, dataelaborazione)
    	}
        ret
  	}



  /**
   * Esegue la validazione di congruenza tra il nome del file xml e i dati contenuti in esso.
   * @param rdd rappresenta il file xml.
   * @return un valore booleano corrispondente alla validazione e un messaggio d'errore.
   */
  def validation(rdd: (Boolean,(String,String),String)) : (Boolean,(String,String),String) = {
		if(rdd._1){
			try{
				    val nome_file = if(rdd._2._1.lastIndexOf("/") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("/")+1).split("_")
						else if (rdd._2._1.lastIndexOf("\\") > -1) rdd._2._1.substring(rdd._2._1.lastIndexOf("\\")+1).split("_")
						else rdd._2._1.split("_")
						
						val noBom = if(rdd._2._2.startsWith(UTF8_BOM)){
						  rdd._2._2.substring(1)
						}else{
						  rdd._2._2
						}
		
				    val xml = XML.loadString( noBom )
						
						//informazioni nome file versione prototipo
						val PIvaDistributoreNF = nome_file(0)
						val PIvaUtenteNF = nome_file(1)
						val MeseAnnoNF = nome_file(2)
						val CodContrDispNF_ = nome_file(5).split("\\.")(0) takeRight 6

						//informazioni tag xml
						val PIvaDistributoreTX = ((xml \\ "IdentificativiFlusso" \\ "PIvaDistributore") text)
						val PIvaUtenteTX = (xml \\ "IdentificativiFlusso" \\ "PIvaUtente") text
						val CodContrDispTX = (xml \\ "IdentificativiFlusso" \\ "CodContrDisp") text

						val dataNf = MeseAnnoNF.substring(0,6)
						val datiPod = (xml \\ "FM" \\ "DatiPod")
						val dpRes2 = datiPod.theSeq.map(y => ( (y \\ "MeseAnno").text).split("/") ).map(x => (x(1) + x(0)) ).filter(!_.equals(dataNf) ) 

						val isValid2 = if(PIvaDistributoreNF != PIvaDistributoreTX){
							(false,"PIvaDistributoreNF")
						}else if(PIvaUtenteNF != PIvaUtenteTX){
							(false,"PIvaUtenteNF")
						}else if(CodContrDispNF_ != CodContrDispTX){
							(false,"CodContrDispNF")
						}else if(dpRes2.length != 0){
							(false,"MeseAnnoNF")
						}else{
							(true,"OK")
						}

    				if(isValid2._1){
    					(isValid2._1, rdd._2, "OK")
    				}else{
    					(false, (rdd._2._1, "nome file xml "+ isValid2._2 + " dati non congruenti"), "002")
    				}
			}catch{
  			case e: Exception => 
			    (false, (rdd._2._1, e.getMessage), "003")
			}
		}else{
			rdd
		}
}

/**
 * identifica i file di misura del nuovo formato 2G
 */
	def isNuovo2G(regex:String, in:String) : Boolean = {
	  val s = regex.split(",")
	  var ret:Boolean = false
	  for(e <- s) if(in.contains(e)){
	    ret=true
	  }
	  ret
	}

  /**
   * Valida la consistenza file xml tramite dei XSD in termini di vincoli: quali elementi e attributi possono apparire, 
   * in quale relazione reciproca, quale tipo di dati può contenere; al fine di accertare se i tipi di dati appartengono al documento xml. 
   * @param rdd rappresenta il file xml.
   * @return un valore booleano corrispondente alla validazione e un messaggio d'errore.
   */
  def validationXSD(rdd: (String,String) ) : (Boolean,(String,String),String) = {
		try{
    			val factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)//CRLF e LF
    			
    			val nomeFile = rdd._1
    			
    			val f1:Source = if(isNuovo2G(regex_2G, nomeFile)){
    			  new StreamSource(defdomplextypes_2G)
    			}else{
    			  new StreamSource(defdomplextypes)
    			}
    			
    			val f2:Source = if(isNuovo2G(regex_2G, nomeFile)){
    			  new StreamSource(defsimpletypes_2G)
    			}else{
    			  new StreamSource(defsimpletypes)
    			}

    			val f3 =  if(nomeFile.contains("_RFO_") && nomeFile.contains( v2_2G )){
    			  new StreamSource(flusso1_rfo_v2)
    			}else if(nomeFile.contains("_RFO_")){
    			  new StreamSource(flusso1_rfo)
    			}else if(nomeFile.contains( v2_2G )){
    			   new StreamSource(flusso1_pdo_v2)
    			}else if(nomeFile.contains("_PDO2G_")){
    			  new StreamSource(flusso_pdo_2G)
    			}else if(nomeFile.contains("_RFO2G_")){
    			  new StreamSource(flusso_rfo_2G)
    			}else{
    			  new StreamSource(flusso1_pdo)
    			}


			
					
					try{
					  val sourceList = Array(f3,f1,f2)
					  val schema = factory.newSchema( sourceList )
					  val validator = schema.newValidator()
					  val ss = new StreamSource(new File( rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/") ))
					  validator.validate(ss)
          }catch{
          		case e: Exception => 
          		  val err = e.getMessage()
          		  if(err.contains("PIVAType")){
          		    
          		  val f11:Source = if(isNuovo2G(regex_2G, nomeFile)){
          			  new StreamSource(defdomplextypes_2G_straniere)
          			}else{
          			  new StreamSource(defdomplextypes_straniere)
          			}
          			
          			val f22:Source = if(isNuovo2G(regex_2G, nomeFile)){
          			  new StreamSource(defsimpletypes_2G_straniere)
          			}else{
          			  new StreamSource(defsimpletypes_straniere)
          			}
          		    
          		    
          		    val f33 =  if(rdd._1.contains("_RFO_") && rdd._1.contains( v2_2G )){
            			  new StreamSource(flusso1_rfo_v2_straniere)
            			}else if(rdd._1.contains("_RFO_")){
            			  new StreamSource(flusso1_rfo_straniere)
            			}else if(rdd._1.contains( v2_2G )){
            			   new StreamSource(flusso1_pdo_v2_straniere)
            			}else if(nomeFile.contains("_PDO2G_")){
            			  new StreamSource(flusso_pdo_2G_straniere)
            			}else if(nomeFile.contains("_RFO2G_")){
            			  new StreamSource(flusso_rfo_2G_straniere)
            			}else{
            			  new StreamSource(flusso1_pdo_straniere)
            			}
          		    
          		    val newFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI)
          		    val newSourceList = Array(f33,f11,f22)
						      val newSchema = newFactory.newSchema( newSourceList )
						      val newValidator = newSchema.newValidator()
						      val newSs = new StreamSource(new File( rdd._1.replaceFirst("file:///", "/").replaceFirst("file:/", "/").replaceFirst("file://", "/") ))
					        newValidator.validate(newSs)
          		  }else{
          		    throw new Exception(e)
          		  }
          }
					
          (true,rdd,"OK")
		}catch{
  		case e: Exception => 
  		  e.printStackTrace()
  		  (false, (rdd._1, e.getMessage() ),"001")
		}
  }


  /**
   * Il metodo main è convenzionalmente stabilito come punto di partenza per l'esecuzione del programma. Vengono istanziate le classi che accedono al contesto di Cloudera.
   * @param args contiene le opzioni che vengono passate al programma Scala da riga di comando.
   */
  def main(args: Array[String]) {

      val commandLineOptions = new CommandLineOptions()
      val commonsCliUtils = new CommonsCliUtils()
    	val commandLine:CommandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)
			val argsObjMaster = new CommonsCliUtils().getArgs(commandLine)
			

			log.info("***** Inizio processo " + argsObjMaster.appName + " *****")

			val conf = new SparkConf()
  			.setAppName( argsObjMaster.appName )
  			.set("spark.shuffle.service.enabled", "false")
  			.set("spark.dynamicAllocation.enabled", "false")
  			.set("spark.io.compression.codec", "snappy")
  			.set("spark.rdd.compress", "true")
  			.set("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
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
						val dataelaborazione = new java.sql.Timestamp( System.currentTimeMillis() )
				

						log.info("*** sc.master: " + sc.master)
						log.info("*** injectionTmp: " + injectionTmp)
						log.info("*** rootDir: " + rootDir)
						log.info("*** minPartitions: " + minPartitions)
						log.info("*** slash: " + slash)
						//hdfs
						log.info("*** quartiPDO: " + quartiPDO)
						log.info("*** report: " + report)
						log.info("*** pdo_rfo: " + pdo_rfo)
						log.info("*** isAggiorna: " + isAggiorna)
						
						//XSD
						log.info("*** defdomplextypes: " + defdomplextypes)
						log.info("*** defsimpletypes: " + defsimpletypes)
						log.info("*** flusso1_pdo: " + flusso1_pdo)
						log.info("*** flusso1_rfo: " + flusso1_rfo)
						log.info("*** flusso1_pdo_v2: " + flusso1_pdo_v2)
						log.info("*** flusso1_rfo_v2: " + flusso1_rfo_v2)
						log.info("*** dataelaborazione: " + dataelaborazione)
						
						//perdita di tensione
						log.info("*** perdita_380: " + perdita_380)
						log.info("*** perdita_220: " + perdita_220)
						log.info("*** perdita_150: " + perdita_150)
						log.info("*** perdita_1_35: " + perdita_1_35)
						log.info("*** perdita_1: " + perdita_1)

						
						
  					val tmpRootDir = s"${injectionTmp}${File.separator}${rootDir}"
  					
  					val elencoUDDTmp:List[String] = scansionaUDD(tmpRootDir)
  					val rddUDDTmp = sc.parallelize(elencoUDDTmp, minPartitions.toInt).setName("Scansiona alberatura temporanea")
  					rddUDDTmp.cache()
					  
					  val lists = rddUDDTmp.mapPartitions{partition => 
					    val ret = partition
					                .map(rddUDDTmpDir => leggiPathXml( new File(rddUDDTmpDir) ))
					                .flatMap(f => f)
					      ret
					  }.collect().map(xml => "file://" + slash +  xml.getPath).toList
					  
      			val rddValidAll = sc.wholeTextFiles(lists.mkString(","), minPartitions.toInt).setName("Acquisizione files di misurazione")
      			log.info(s"*** WholeTextFiles OK")
  
  					
  					val rddXsd = rddValidAll.map(validationXSD( _ )).setName("Validazione xsd")
  					log.info("***** ValidationXSD OK")
  
  					val rdd2 = rddXsd.map(validation).setName("Validazione nome file")
  					rdd2.cache()
  					log.info("***** Validazione nome file OK")
  
  
  					val validPdo = rdd2.filter( _._1 ).flatMap( letturaEValidazioni( _ , dataelaborazione) ).setName("Lettura e validazioni")
  					log.info("***** lettura e validazioni OK")
  					
  
  					val dfQS1 = sqlCtx.createDataFrame(validPdo, schemaQuarti)
  					log.info("***** creazione DataFrame schema quarti OK")


						dfQS1
							.write
							.format("parquet")
							.mode(SaveMode.Append)
							.partitionBy("annoquarti","mesequarti","pivadistributorequarti", "codcontrdispquarti", "areaquarti")
							.save(quartiPDO)
							log.info("***** insert misure quarti OK")
							

					 /*
						* INSERT REPORT
						*/
						val rdd2NoValid = rdd2.filter( !_._1 ).map(f => Row(f._3, f._2._1, f._2._2, dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt))
						log.info("***** filtro Report file non validi OK")

						val dfQS3 = sqlCtx.createDataFrame(rdd2NoValid, schemaReport)      
						log.info("***** creazione DataFrame Report misure non valide OK")
							

						dfQS3
							.write
							.format("parquet")
							.mode(SaveMode.Append)
							.partitionBy("annomese")
							.save(report)
						log.info("***** insert Report misure non valide OK")


						val rddValid = rdd2.filter( _._1 ).map(f => Row("000", f._2._1, "OK", dataelaborazione, (argsObjMaster.anno + argsObjMaster.mese).toInt))
						log.info("***** filtro Report file validi OK")

						val dfQS4 = sqlCtx.createDataFrame(rddValid, schemaReport)       
						log.info("***** creazione DataFrame Report misure valide OK")

						dfQS4
							.write
							.format("parquet")
							.mode(SaveMode.Append)
							.partitionBy("annomese")
							.save(report)
						log.info("***** insert Report misure valide OK")
							
						/*
						 * aggiorno le partizioni
						 */
							
						if (!commandLine.hasOption(commandLineOptions.local.getOpt)) {
							val hiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
							hiveContext.sql("MSCK REPAIR TABLE au.fm_quarti")
							hiveContext.sql("MSCK REPAIR TABLE au.report_ingestione")
							log.info("***** aggiornamento partizioni OK")
						}
							
      				
          }catch{
            case ex: FileNotFoundException => ex.printStackTrace()
            case e: Exception =>  e.printStackTrace()
          }finally{
          	 sc.stop()
          }

					log.info(s"***** Fine processo ${argsObjMaster.appName} *****")
      }

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
			 
    def leggiPathXml(f:File): Array[File] = {
      val ret = f.listFiles()
      if(ret(0) != null && ret(0).isDirectory() ){
        ret.map(xf => leggiPathXml(xf) ).flatMap(f => f)
      }else{
        ret
      }
		}
    
    def scansionaUDD(injectionPath:String) : List[String] = {
			val abW2 = new ArrayBuffer[(String)]()

			//cartella principale 
			for (xmlPrincipaleDir <- new File(injectionPath).listFiles()) {
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
        		case e: Exception =>  e.printStackTrace()
        	}
				}
			}
			abW2.toList
	  }
}
