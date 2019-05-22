package scalaproject
//ceci est un commentaire

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import java.io.{BufferedWriter, FileWriter}
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.util.Random
import au.com.bytecode.opencsv.CSVWriter
import org.apache.spark.rdd.PairRDDFunctions

//ceci est un commentaire 
  case class Sales(product_id:String,time_id:String,customer_id:String)
  case class Customer(customer_id:String, account_num:String, last_name:String)
  
object CreatingSparkContext {
  
  def main(args:Array[String]): Unit = {
    
    val sparkConf = new SparkConf()

    sparkConf.setAppName("jobSpark")
    
    sparkConf.setMaster("local")
    val sc = new SparkContext(sparkConf)
   
    
   /*creation des objets customer*/
    
    val customer = sc.textFile("C:/data/customer.csv").filter(!_.contains("customer_id")).map {line =>
      val p = line.split(";").map(_.trim)
      Customer(p(0),p(1),p(2))
      
         }
    
    //customer.take(10).foreach(println)
    
    /*creation des objets customer*/
    
    val sales = sc.textFile("C:/data/sales.csv").filter(!_.contains("product_id")).map {l =>
      val q = l.split(";").map(_.trim)
      Sales(q(0),q(1),q(2))
      
         }
    
   //sales.take(10).foreach(println)
    
    val salesRDD = sales.map(f => f.customer_id -> (f.product_id,f.time_id))
    val customerRDD = customer.map(s => s.customer_id -> (s.last_name,s.account_num))
    
    val RddJoin = salesRDD.join(customerRDD)
    
    RddJoin.saveAsTextFile("C:/data/salCSV")
    
   
    
    
    
    
    
    

  
  
  
  
  }
  }
