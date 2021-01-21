package it.au.misure.cli

import java.io.PrintWriter
import java.util.{Calendar,TimeZone,GregorianCalendar}
import java.util.Properties

import scala.util.{Failure, Success, Try}

import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import it.au.misure.util.CreateProperties

import org.apache.log4j.{Level, LogManager}

/**
 * Command line utils leveraging Apache Commons CLI
 */
@SerialVersionUID(114L)
class CommonsCliUtils  extends Serializable{
  
  /**
   * Contiene le proprietà del file job.properties
   */
  val prop:Properties = new CreateProperties().prop
  
  /**
   * Legge e acquisisce le opzioni passate a riga di comando.
   * @param args argomenti passati
   * @param options rappresenta le opzioni disponibili.
   * @return oggetto che gestisce le varie opzione previste.
   */
	def parseArgsList(args: Array[String], options: Options): CommandLine = {
    
			Try {
				new DefaultParser().parse(options, args)
			} match {
			case Success(settings) => settings
			case Failure(e) => printHelpForOptions(options); throw e
			}
	}

  /**
   * In caso di errata opzione, mostra a video le opzioni disponibili.
   * @param options rappresenta le opzioni disponibili.
   */
	def printHelpForOptions(options: Options) {
		val f = new HelpFormatter()
				f.setWidth(100)
				f.printHelp("fm", "", options, "", true)
	}

	/**
	 * Utilità per la definizione dei giorni richiesti al processo.
   * @param commandLine oggetto che gestisce le varie opzione previste.
   * @param commandLineOptions contiene le varie opzioni previste.
   * @return List elenco dei giorni. 
	 */
	def getGiorni(commandLine: CommandLine, commandLineOptions:CommandLineOptions): List[String] = {
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val giorni:String = commandLine.getOptionValue(commandLineOptions.giorni.getOpt)
	   val mese:String = if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.mese.getOpt)
	   }else{
	     null
	   }
	   val ret:List[String] = getGiorni(mese, giorni)
	  ret
	}

/**
 * Genera gli argomenti utilizzati in fase di decompressione e ingestione.
 * @param commandLine oggetto che gestisce le varie opzione previste.
 * @param commandLineOptions contiene le varie opzioni previste.
 * @param giornoIn giorno per il quale creare gli argomenti necessari.
 * @return Args argomenti.
 */
def getArgsRange(commandLine: CommandLine, commandLineOptions:CommandLineOptions, giornoIn:String): Args = {
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   
	  try{
	   if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	     calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	   }
	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     //i mesi iniziano da 0: gen=0, feb=1,...
	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	     calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	   }
	     
	   cal.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
	   calXml.set(Calendar.DAY_OF_MONTH, giornoIn.toInt);
	    
	  }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	  }

	   
	   val annomesegiornodir:Int =  ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt

	   val nomeFile = {
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + "01"
	   }
	   
	   val nomeFile2G = {
	     //prendo solo i file del giorno precedente
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + ("0" + calXml.get(Calendar.DAY_OF_MONTH) takeRight 2 )
	   }
	   

		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 val giorno:String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2
     
		 Args(anno, mese, giorno, null, null, null, null, null,  null, nomeFile, nomeFile2G, null, null, annomesegiornodir)
}

/**
 * Genera gli argomenti utilizzati in fase di aggreagazione.
 * @param commandLine oggetto che gestisce le varie opzione previste.
 * @return Args argomenti
 */
def getArgsAggregati(commandLine: CommandLine): Args = {
  
    val commandLineOptions = new CommandLineOptions()
    
	   val timeZone = prop.getProperty("spark.app.time_zone")
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
//	   val calAggr = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   
	  try{
	   if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	     calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
	   }
	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
	     //i mesi iniziano da 0: gen=0, feb=1,...
	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	     calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
	   }
	     
	    
	  }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	  }

	   
	    val annomesegiornodir:Int = if (commandLine.hasOption(commandLineOptions.annomesegiornodir.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.annomesegiornodir.getOpt).toInt
	   }else{
	     ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt
	   }
	   

		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 
		 val appName:String = if(commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt)){
			 "FM Aggregazione Misure Orarie"
		 }else if(commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)){
			 "FM Aggregazione Am Misure Master"
		 }else{
			 "X"
		 }
	   
		 val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
			 "local[*]"
		 }else{
			 "yarn-client"
		 }
     
		 Args(anno, mese, null, null, appName, null, null, null, master, null, null, null, null, annomesegiornodir)
}
	
	def getArgs(commandLine: CommandLine): Args = {
	  val commandLineOptions = new CommandLineOptions()
	  
	   val timeZone = prop.getProperty("spark.app.time_zone")
	  
	   val cal = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calXml = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   val calGhigliottina = Calendar.getInstance(TimeZone.getTimeZone( timeZone ));
	   
	   
	   try{
	     if (commandLine.hasOption(commandLineOptions.anno.getOpt)) {
  	     cal.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
  	     calXml.set(Calendar.YEAR, commandLine.getOptionValue(commandLineOptions.anno.getOpt).toInt);
  	   }
  	   if (commandLine.hasOption(commandLineOptions.mese.getOpt)) {
  	     //i mesi iniziano da 0: gen=0, feb=1,...
  	     cal.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
  	     calXml.set(Calendar.MONTH, (commandLine.getOptionValue(commandLineOptions.mese.getOpt).toInt) - 1);
  	   }
  	   if (commandLine.hasOption(commandLineOptions.giorno.getOpt)) {
  	     cal.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
  	     calXml.set(Calendar.DAY_OF_MONTH, commandLine.getOptionValue(commandLineOptions.giorno.getOpt).toInt);
  	   }else{
  	     // in tutti i casi devo prendere la cartella del giorno precedente di quando parte il processo
  	     cal.add(Calendar.DAY_OF_MONTH, -1)
  	     calXml.add(Calendar.DAY_OF_MONTH, -1)
  	   }
	   }catch{
	    case e: Exception => throw new Exception("Inseriti valori non validi", e)
	   }
	   
	   
	   val annomesegiornodir:Int = if (commandLine.hasOption(commandLineOptions.annomesegiornodir.getOpt)) {
	     commandLine.getOptionValue(commandLineOptions.annomesegiornodir.getOpt).toInt
	   }else{
	     ("" + calGhigliottina.get(Calendar.YEAR) + ("0" + (calGhigliottina.get(Calendar.MONTH) + 1) takeRight 2 ) + 16).toInt
	   }

	   
	   val nomeFile = {
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 )
	   }
	   
	   val nomeFile2G = {
	     //prendo solo i file del giorno precedente
	     "" + calXml.get(Calendar.YEAR) + ("0" + (calXml.get(Calendar.MONTH) + 1) takeRight 2 ) + ("0" + calXml.get(Calendar.DAY_OF_MONTH) takeRight 2 )
	   }
	   

		 val anno:String = Integer.toString(cal.get(Calendar.YEAR))
		 val mese:String = "0" + Integer.toString(cal.get(Calendar.MONTH) + 1) takeRight 2
		 val giorno:String = "0" + Integer.toString(cal.get(Calendar.DAY_OF_MONTH)) takeRight 2
     
						val pdo_rfo:String = if (commandLine.hasOption(commandLineOptions.pdo.getOpt)){
							"PDO"
						}else if (commandLine.hasOption(commandLineOptions.rfo.getOpt)){
							"RFO"
						}else {
							"PDO"
						}


						val appName:String = if(commandLine.hasOption(commandLineOptions.injection.getOpt)){
  						  if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
  							  "Injection Misure Quarti 2G " + pdo_rfo
  						  }else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
  							  "Injection Misure Quarti 1G " + pdo_rfo
  						  }else{
  						    "Injection Misure Quarti"
  						  }
  						}else if(commandLine.hasOption(commandLineOptions.decomprime2G.getOpt)){
  						  if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
  							  "Decompressione Misure Quarti 2G " + pdo_rfo
  						  }else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
  							  " Decompressione Misure Quarti 1G " + pdo_rfo
  						  }else{
  						    "Decompressione Misure Quarti " + pdo_rfo
  						  }
  						}else if(commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt)){
  							"Aggregazione Misure Orarie"
  						}else if(commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)){
  							"Aggregazione Am Misure Master"
  						}else if(commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)){
  							"Aggiornamenti"
  						}else if(commandLine.hasOption(commandLineOptions.test.getOpt)){
  							"Test"
  						}else{
  							"X"
  						}
						
						/*
						 * cartella temporanea dove vengono decompressi i file di misura zippati
						 */
						val tmpDir:String = if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
							prop.getProperty("spark.app.directory.temporanea.2G") // /mnt/isilonshare1/TMP_2G
						}else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
							prop.getProperty("spark.app.directory.temporanea.1G") // /mnt/isilonshare1/TMP_1G_collaudo0720
						}else{
						  ""
						}
						
						/*
						 * cartella root dei file di misura zippati
						 */
						val rootDir:String = if(commandLine.hasOption(commandLineOptions.injection2G.getOpt)){
							prop.getProperty("spark.app.directory.root.2G") // isilonshare
						}else if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)){
							prop.getProperty("spark.app.directory.root.1G") // Test_clouderaShare
						}else{
						  ""
						}
						 
						
							val logLevel:String = if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
							  LogManager.getRootLogger.setLevel(Level.DEBUG)
								"DEBUG"
							}else{
							  LogManager.getRootLogger.setLevel(Level.ERROR)
								"ERROR"
							}
							
							val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
								"local[*]"
							}else{
								"yarn-client"
							} 

							Args(anno, mese, giorno, pdo_rfo, appName, logLevel, tmpDir, rootDir,  master, nomeFile, nomeFile2G, null, null, annomesegiornodir)
	}
	
	def getGiorni(mese:String, giorno:String) : List[String] = {
		val cal = new GregorianCalendar();
		if(mese != null){
		  cal.set(Calendar.MONTH, Integer.parseInt(mese) - 1);
		}
		
		val ags = giorno.split(",");
		
		val maxDay = cal.getActualMaximum(Calendar.DAY_OF_MONTH);
		val minDay = 1;
		val startsWith = giorno.startsWith(",");
		val endsWith = giorno.endsWith(",");
		val isEmpty = giorno.isEmpty();
		val length = ags.length;
		
		val listaGiorni:List[String] = if(isEmpty){ //nessun valore
		  println("Nessun valore valido")
		  Nil
		}else if(startsWith && endsWith){ // , -> dal primo all'ultimo giorno del mese
		  val eaValues = (1 to maxDay).map( "0" + _ takeRight 2 )
		  eaValues.toList
		}else if(endsWith){ //dall'ultimo giorno specificato a fine mese
		  val eaValues = (ags( ags.length - 1 ).toInt + 1 to maxDay).map { "0" + _.toString takeRight 2 }.toList
		  ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList ++ eaValues
		}else if(startsWith){ // dal primo giorno del mese al primo giorno specificato
      val eaValues = (1 to (ags(1).toInt - 1)).map { "0" + _.toString() takeRight 2 }.toList
      eaValues ++ ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList
		}else {// prendo i giorni specificati
		  ags.filter( _.length > 0).map( "0" + _ takeRight 2 ).toList
		}
		
		listaGiorni
  }

case class Args(anno:String, mese:String, giorno:String, PdoRfo:String, appName:String, logLevel:String, injectionTmp:String, rootDir:String, master:String, nomeFile:String, nomeFile2G:String, meseAggr:String, annoAggr:String, annomesegiornodir:Int)


}