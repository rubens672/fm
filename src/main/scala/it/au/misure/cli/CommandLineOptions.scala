package it.au.misure.cli

import it.au.misure.commons.cli.{OptionGroup, Options, Option => CliOption}


/**
	* Commons-cli command line options for the FM tool suite.
	*/
@SerialVersionUID(114L)
class CommandLineOptions extends Serializable{
	// base options
	def version: CliOption = new CliOption("v", "version", false, "Stampa la versione e esci")
	def help: CliOption = new CliOption("h", "help", false, "Stampa l'help ed esci")
	def verbose: CliOption = new CliOption("V", "verbose", false, "Abilita log dettagliati (livello di log a debug)")
	
	//AU
	def decomprime2G: CliOption = new CliOption("D", "decomprimi", false, "Decomprime gli archivi 1G/2G su una tabella temporanea")
	def noDec: CliOption = new CliOption("n", "noDec", false, "Legge le directory non compresse")
	
	def injection: CliOption = new CliOption("i", "ingestione", false, "Ingestione dei flussi misura")
	def injection1G: CliOption = new CliOption("g", "1g", false, "Specifica l'ingestione dei flussi 1G")
	def injection2G: CliOption = new CliOption("G", "2g", false, "Specifica l'ingestione dei flussi 2G")
	def pdo: CliOption = new CliOption("t", "pdo", false, "Acquisisce misure PDO")
	def rfo: CliOption = new CliOption("T", "rfo", false, "Acquisisce misure RFO")
	def aggregatiOrari: CliOption = new CliOption("a", "aggregah", false, "Aggrega in misure orarie")
	def aggregatiAM: CliOption = new CliOption("b", "aggregaam", false, "Aggrega per AM")
	def aggiornamento: CliOption = new CliOption("u", "aggiorna", false, "Aggiorna le misure")
	def test: CliOption = new CliOption("k", "test", false, "Funzione di test")
	
	def distrAgg: CliOption = {
	  val o = new CliOption("p", "disag", true, "pivadistributore in aggregati orari o n_id_distr in AM su cui generare l'aggregato")
	  o.setArgName("DISTR_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def uteAgg: CliOption = {
	  val o = new CliOption("P", "uteag", true, "pivautente in aggregati orari o n_id_udd in AM su cui generare l'aggregato")
	  o.setArgName("UTE_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def noDistrAgg: CliOption = {
	  val o = new CliOption("q", "nodisag", true, "pivadistributore in aggregati orari o n_id_distr in AM cui non generare l'aggregato")
	  o.setArgName("DISTR_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def noUteAgg: CliOption = {
	  val o = new CliOption("Q", "nouteag", true, "pivautente in aggregati orari o n_id_udd in AM cui non generare l'aggregato")
	  o.setArgName("UTE_LIST")
		o.setOptionalArg(false)
	  o
	}
	
	def creaFM: CliOption = {
	  val o = new CliOption("f", "creflusso", true, "crea un numero arbitrario di flsussi misure con pod casuale")
	  o.setArgName("NUM_POD")
		o.setOptionalArg(false)
	  o
	}
	
	def distrUteAgg: CliOption = {
	  val o = new CliOption("d", "disute", true, "Acquisisce la lista di distributori utente specificati")
	  o.setArgName("DISUTE_LIST")
		o.setOptionalArg(false)
	  o
	}  
	
	def annomesegiornodir: CliOption = {
	  val o = new CliOption("z","ghigliottina", true, "Termine espresso come AAAAMMGG da cui considerare le aggregazioni. Default anno corrente <AAAA>, mese precedente <MM>, giorno 16 del mese")
	  o.setArgName("AAAAMMGG")
		o.setOptionalArg(false)
		o
	}
	
	def anno: CliOption = {
	  val o = new CliOption("y", "anno", true, "Anno di riferimento")
	  o.setArgName("AAAA")
		o.setOptionalArg(false)
		o
	}
	def mese: CliOption = {
	  val o = new CliOption("m", "mese", true, "Mese di riferimento")
	  o.setArgName("MM")
		o.setOptionalArg(false)
		o
	}
	def giorno: CliOption = {
	  val o = new CliOption("s", "giorno", true, "Giorno di riferimento")
	  o.setArgName("GG")
		o.setOptionalArg(false)
		o
	}
	
	def giorni: CliOption = {
	  val o = new CliOption("S", "giorni", true, "Sequenza di giorni di riferimento: \n" + 
	    "<G,G,G,G> solo i giorni specificati, \n " +
      "<,G,G,G>  dal primo del mese al primo della lista seguito dai giorni specificati \n" +
      "<G,G,G,>  i giorni specificati fino a fine mese \n" + 
  	  "<G>       singolo giorno \n" +
      "<,G>      dal primo del mese al giorno specificato \n" +
      "<G,>      dal giorno specificato fino alla fine del mese \n" + 
      "<,>       dal primo giorno del mese all'ultimo "
	  )
	  o.setValueSeparator(',')
	  o.setArgName("G,G")
		o.setOptionalArg(false)
		o
	}

	def local: CliOption = new CliOption("l", "locale", false, "Il processo viene eseguito localmente ( local[*] )")
	
	def aggrCommands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(distrUteAgg)
		og.addOption(distrAgg)
		og.addOption(uteAgg)
		og.addOption(noDistrAgg)
		og.addOption(noUteAgg)
		og
	}
	
	// option group for commands, as they are mutually exclusive
	def commands: OptionGroup = {
		val og = new OptionGroup()
		og.addOption(decomprime2G)
		og.addOption(injection)
		og.addOption(aggregatiOrari)
		og.addOption(aggregatiAM)
		og.addOption(creaFM)
		og.addOption(aggiornamento)
		og.addOption(test)
	}
	
  def commandsPdoRfo: OptionGroup = {
		val os = new OptionGroup()
		os.addOption(pdo)
		os.addOption(rfo)
	}
	
	def commandsGiorni: OptionGroup = {
		val os = new OptionGroup()
		os.addOption(giorno)
		os.addOption(giorni)
	}
	
	// final options
	def getOptions: Options = {
		val os = new Options()
		os.addOption(version)
		os.addOption(help)
		os.addOption(verbose)
		os.addOption(anno)
		os.addOption(mese)
		os.addOption(local)
		os.addOption(annomesegiornodir)
		os.addOption(injection1G)
		os.addOption(injection2G)
		os.addOptionGroup(commands)
		os.addOptionGroup(commandsGiorni)
		os.addOptionGroup(commandsPdoRfo)
		os.addOptionGroup(aggrCommands)
		os
	}
	
}

