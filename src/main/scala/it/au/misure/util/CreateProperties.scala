package it.au.misure.util

import org.apache.spark._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import com.typesafe.config._

import org.apache.hadoop.conf.Configuration

import java.io.File
import java.io.FileInputStream
import java.sql.{Connection, DriverManager}
import oracle.jdbc.driver.OracleDriver

import scala.util.{Failure, Success, Try}


import java.util.Properties;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import java.io.InputStream;


/*
 * https://stackoverflow.com/questions/31115881/how-to-load-java-properties-file-and-use-in-spark
 * 
 * job.property -> app.name=xyz
 * 
 * spark-submit --properties-file  job.property ...
 * 
 * spark-submit --files job.properties ...
 * 
   import java.util.Properties;
   import org.apache.hadoop.fs.FSDataInputStream;
   import org.apache.hadoop.fs.FileSystem;
   import org.apache.hadoop.fs.Path;
   import org.apache.spark.SparkFiles;
   
   //Load file to propert object using HDFS FileSystem
   String fileName = SparkFiles.get("job.properties")
   Configuration hdfsConf = new Configuration();
   FileSystem fs = FileSystem.get(hdfsConf);
   
   //THe file name contains absolute path of file
   FSDataInputStream is = fs.open(new Path(fileName));
   Properties prop = new Properties();
   //load properties
   prop.load(is)
   //retrieve properties
   prop.getProperty("app.name");
 * 
 */

import java.util.Properties;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.SparkFiles;
import org.apache.hadoop.conf.Configuration;


/**
 * ==CreateProperties== 
 * Utilità per la lettura delle configurazioni su file system hdfs.
 */
class CreateProperties {
  val envVar = s"/apps/deploy" 

  /**
   * Contiene le proprietà in formato chiave valore del file job.properties
   * @return Properties 
   */
  def prop:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    
    val is = fs.open(new Path(s"${ envVar }/job.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }
  
    /**
   * Contiene le proprietà in formato chiave valore del file query.properties
   * @return Properties 
   */
  def query:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    
    val is = fs.open(new Path(s"${ envVar }/query.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }
  
  /**
   * Contiene le proprietà in formato chiave valore del file xsd.properties
   * @return Properties 
   */
    def xsdProp:Properties = {
    val hdfsConf = new Configuration()
    val fs = FileSystem.get(hdfsConf)
    
    val is = fs.open(new Path(s"${ envVar }/xsd.properties"));
    val prop = new Properties();
    prop.load(is)
    prop
  }
  
}