package it.au.misure.util

import java.util.zip.ZipFile
import java.util.zip.ZipInputStream
import java.io.FileInputStream
import java.io.FileOutputStream
import scala.collection.JavaConversions._
import java.util.zip.ZipEntry
import java.io.InputStream
import java.io.OutputStream
import java.io.File

/**
 * ==ZipArchive==
 * Utilizzato per decomprimere i file di misura in formato Zip.
 */
class ZipArchive extends LoggingSupport{

	val BUFSIZE = 4096
			val buffer = new Array[Byte](BUFSIZE)

			/**
			 * Decomprime tutti i file in formato Zip
       * @param source archivio zip
       * @param targetFolder cartella di destinazione file decompresso
			 */
			def unZip(source: String, targetFolder: String) = {
					try{
						val zipFile = new ZipFile(source)
//						val zis = new ZipInputStream(

								unzipAllFile(zipFile.entries.toList, getZipEntryInputStream(zipFile)_, new File(targetFolder))
						zipFile.close()
					}catch{
					case e: Exception =>  {
					  log.error(s"${e.getMessage} - source: ${source} - targetFolder: ${targetFolder}", e)
//					  throw new Exception(e)
					}
					}

	}

	/**
	 * Retituisce il flusso di ingresso per la lettura del contenuto del file zip specificato.
	 * @param zipFile classe utilizzata per leggere le voci da un file zip
	 */
	def getZipEntryInputStream(zipFile: ZipFile)(entry: ZipEntry) = zipFile.getInputStream(entry)

			/**
			 * Decomprime l'elenco di file in formato Zip
       * @param source archivio zip
       * @param targetFolder cartella di destinazione file decompresso
			 */
			def unzipAllFile(entryList: List[ZipEntry], inputGetter: (ZipEntry) => InputStream, targetFolder: File): Boolean = {

					entryList match {
					case entry :: entries =>

					if (entry.isDirectory)
						new File(targetFolder, entry.getName).mkdirs
						else
							saveFile(inputGetter(entry), new FileOutputStream(new File(targetFolder, entry.getName)))

							unzipAllFile(entries, inputGetter, targetFolder)
					case _ =>
					true
					}

			}

	/**
	 * Scrive i file decompressi su file system. 
	 * @param fis flusso di byte in ingresso
	 * @param fos flusso di byte in uscita
	 */
			def saveFile(fis: InputStream, fos: OutputStream) = {
					writeToFile(bufferReader(fis)_, fos)
					fis.close
					fos.close
			}

			
			def bufferReader(fis: InputStream)(buffer: Array[Byte]) = (fis.read(buffer), buffer)

					def writeToFile(reader: (Array[Byte]) => Tuple2[Int, Array[Byte]], fos: OutputStream): Boolean = {
							val (length, data) = reader(buffer)
									if (length >= 0) {
										fos.write(data, 0, length)
										writeToFile(reader, fos)
									} else
										true
					}
}