package it.au.misure.cli;

import org.apache.log4j.{Level, LogManager}
import it.au.misure.ingestione.Injection
import org.apache.spark.launcher.{SparkAppHandle, SparkLauncher}
import java.util.concurrent.CountDownLatch
import it.au.misure.commons.cli.{CommandLine, DefaultParser, HelpFormatter, Options}
import scala.util.{Failure, Success, Try}
import it.au.misure.ingestione.Decomprime12G
import it.au.misure.util.LoggingSupport

/**
 * ==Tool==
 * Rappresenta il punto di ingresso per utilizzare il processo  da linea di comando. 
 * Implementa la libreria Common CLI di Apache per leggere le opzioni della riga di comando passate al processo. E' anche in grado di stampare
 * messaggi di aiuto che dettagliano le opzioni disponibili. 
 */
object Tool extends LoggingSupport {
  
  val commonsCliUtils = new CommonsCliUtils()
	val commandLineOptions = new CommandLineOptions()
  
	def main(args: Array[String]) {
      
		/*
		 *  parse command line
		 */
	 
		val commandLine = commonsCliUtils.parseArgsList(args, commandLineOptions.getOptions)

		/*
		 * azioni di utilita'
		 */
		utility(commandLine)
		
		/*
		 * run the chosen command
		 */
		try{
		  runChosenCommand(commandLine,args)
		}catch{
		  case e: Exception => {
		    log.error(e.getMessage, e)
		    e.printStackTrace()
		  }
		  System.exit(1)
		}
		
	}

  /**
   * Stampa un messaggio di errore e chiude il programma.
   * @param message messaggio d'errore.
   */
	def printErrorAndExit(message: String): Unit = {
			log.info(message)
			log.info("Digita --help per informazioni")
			System.exit(0)
	}

	 /**
   * Stampa a video la versione del processo e chiude il programma.
   */
	def printVersionAndExit(): Unit = {
			log.info(s" Tool - Versione 1.0.5-SNAPSHOT") 
			System.exit(0)
	}

	 /**
   * Stampa a video le opzioni disponibili del processo e chiude il programma.
   */
	def printHelpAndExit(): Unit = {
			commonsCliUtils.printHelpForOptions(commandLineOptions.getOptions)
			System.exit(0)
	}

  /**
   * Punto di ingresso per le varie funzionalità del processo in base all'argomento passato da riga di comando.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
   */
	def runChosenCommand(commandLine: CommandLine, args: Array[String]): Unit = {
	    val inizio = System.currentTimeMillis() / 1000

			if (commandLine.hasOption(commandLineOptions.decomprime2G.getOpt)) {
			  decomprime2G(commandLine, args)
				
			} else if (commandLine.hasOption(commandLineOptions.injection.getOpt)) {
			  injection(commandLine, args)
			  
			} else if (commandLine.hasOption(commandLineOptions.aggregatiOrari.getOpt)) {
				it.au.misure.aggregazioni.AggregazioneMisureOrarieValidazioni.main(args)
				
			} else if (commandLine.hasOption(commandLineOptions.aggregatiAM.getOpt)) {
				it.au.misure.aggregazioni.AmMisureMaster.main(args)
				
			}  else if (commandLine.hasOption(commandLineOptions.aggiornamento.getOpt)) {
				injection(commandLine, args)
				
			} else if (commandLine.hasOption(commandLineOptions.creaFM.getOpt)) {
				it.au.misure.util.CreaFlusso.main(args)

			} else {
				log.info("Nessun comando specificato! Usa --help per vedere l'uso.")
				printHelpAndExit()
				System.exit(1)
			}
			
			val fine = System.currentTimeMillis() / 1000
			log.info(s"***** tempo esecuzione ${((fine - inizio) / 60)}:${((fine - inizio) % 60)}" )
	}
	
	/**
	 * azioni di utilita'
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 */
	def utility(commandLine:CommandLine) = {
	  // handle version & help
		if (commandLine.hasOption(commandLineOptions.version.getOpt)) {
			printVersionAndExit()
		} else if (commandLine.hasOption(commandLineOptions.help.getOpt)) {
			printHelpAndExit()
		} 

		// enable debug if verbose was specified
		if (commandLine.hasOption(commandLineOptions.verbose.getOpt)) {
			LogManager.getRootLogger.setLevel(Level.DEBUG)
		}
	}
	
	/**
	 * Funzione per la decompressione dei file di misura.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
	 */
	def decomprime2G(commandLine:CommandLine, args: Array[String]) = {
	    val tipoFile:String = if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			    "1G"
			  }else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			    "2G"
			  }else{
			    val msg = "Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso."
			    log.info(msg)
			    printHelpAndExit()
			    System.exit(1)
			    msg
			  }
			  log.info("CommandLineOptions.decomprime " + tipoFile)
				it.au.misure.ingestione.Decomprime12G.main(args)
	}
	
		/**
	 * Funzione per l'ingestione dei file di misura.
	 * @param commandLine oggetto che gestisce le varie opzione previste.
	 * @param args argomenti passati da riga di comando
	 */
	def injection(commandLine:CommandLine, args: Array[String]) = {
	  if(commandLine.hasOption(commandLineOptions.injection1G.getOpt)) {
			    log.info("CommandLineOptions.injection1G")
			  }else if (commandLine.hasOption(commandLineOptions.injection2G.getOpt)) {
			    log.info("CommandLineOptions.injection2G")
			  }else{
			    log.info("Nessun tipo di file (1g/2g) specificato! Usa --help per vedere l'uso.")
			    printHelpAndExit()
				  System.exit(1)
			  }
				it.au.misure.ingestione.Injection.main(args)
				
	}

	/*
	 * local[*]
	 * yarn-client
	 */
	def execute(commandLine: CommandLine, mainClass: String): Unit = {
	  
	   val master:String = if (commandLine.hasOption(commandLineOptions.local.getOpt)) {
				"local[*]" 
			}else{
			  "yarn-client"
			}
	  
			val spark = new SparkLauncher()
					.setAppName(" Inserimento Misure Quarti Tool")
					.setAppResource(SparkLauncher.SPARK_MASTER)
					.setMainClass("it.au.misure.util.Decomprime2G")
					.setMaster(master)
					
					for(e <- commandLine.getOptions) {
					  if(e.getValue == null){
					    spark.addAppArgs("--" + e.getLongOpt)
					  }else{
					     spark.addAppArgs("--" + e.getLongOpt, e.getValue)
					  }
					}
					
					println("startApplication start")
					val handle  = spark.startApplication()

					val countDownLatch = new CountDownLatch(1);

			val listener = new SparkAppHandle.Listener {
				override def infoChanged(handle: SparkAppHandle): Unit = {}
				override def stateChanged(handle: SparkAppHandle): Unit = {

						if (handle.getState().isFinal()) {
							countDownLatch.countDown();
						}

				}
			}
	}

}