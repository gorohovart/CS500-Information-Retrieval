package cs500ir.spark

import java.io.ByteArrayInputStream

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext._
import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import info.bliki.wiki.filter.WikipediaParser
import info.bliki.wiki.model.WikiModel
import io.mindfulmachines.input.XMLInputFormat
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.{PairRDDFunctions, RDD}
import org.htmlcleaner.HtmlCleaner
import org.xml.sax.SAXException
import org.apache.spark.rdd.RDD


import scala.io
import java.io.File

object HelloSpark {
  def main(args: Array[String]): Unit = {
    //val sparkSession = SparkSession
    //  .builder()
    //  .appName("HelloSpark")
    //  .config("spark.master", "local")
    //  .getOrCreate()



    val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
      .set("spark.executor.memory", "10g")
      .set("spark.driver.maxResultSize", "10g")//.set("spark.driver.memory", "2g")

    val sc = new SparkContext( conf )
    val stopWords = readFile("stopWords.txt")

    //stopWords.foreach(x=> println("file:" + x))
    //tokenize("British hurdler Sarah Claxton is confident she can win her first major medal at next month's European Indoor Championships in Madrid.", stopWords).foreach( x=> println("file:" + x))

    val parse = false
    val compute = true

    val tfidfRDD : RDD[(String, (String, Double))] = {
      if (compute) {

        val pages: RDD[(String, String)] =
          if (parse) {
            //val rawpages = readWikiDump(sc, "ruwiki.xml")
            //val pages = parsePages(rawpages).values.map(p => (p.title, p.text))
            val rawpages = readWikiDump(sc, "ruwiki.xml")
            //.top(10)
            val pages = parsePages(rawpages).values.map(p => (p.title, p.text))
            val pagesOnlyWords = pages.map(x => (x._1.filter(x => x != ','), tokenize(x._2, stopWords).mkString(" ")))
            pagesOnlyWords.saveAsTextFile("pages")
            pagesOnlyWords
          }
          else {

            sc.textFile("pages/part-*").map(x => x.split(',')).map(x => (x(0), x(1))) //, classOf[String], classOf[String])
            /*
            val files : List[File] = {
              val d = new File("sport")
              if (d.exists && d.isDirectory) {
                d.listFiles.filter(_.isFile).toList
              } else {
                List[File]()
              }
            }
            sc.parallelize(files.map(f => (f.getName, scala.io.Source.fromFile("sport/" + f.getName, "UTF-8").mkString)))
            */
          }

        val numberOfDocs = pages.count()
        /// term, doc
        val filteredPages =
          pages.flatMap(y => y._2.split(" ").map(s => ((s, y._1), 1))) //.flatMap(x=>x.toSeq)//.toDF()

        //val filteredPages = filteredPages1.flatMap(x=>x.toSeq)

        //println("Terms example:")
        //filteredPages.foreach( x=> println("file:" + x._1+ " term:" + x._2))

        val termFrequency = filteredPages.reduceByKey(_+_)
        termFrequency.saveAsTextFile("termFrequency")
        /*aggregateByKey(0)(
              (acc, _) => acc + 1,
              (acc1,acc2) =>  acc1 + acc2 )*/
        //countByValue

        val documentFrequency = termFrequency.map(x => (x._1._1, 1)).reduceByKey(_+_).collectAsMap()//filteredPages.distinct().map(x => (x._2, x._1))
        //val documentFrequency = documentToTerm.countByKey()

        val tfIdf = termFrequency.flatMap(x => {
          val term = x._1._1
          val doc = x._1._2
          val tf = x._2
          val df = documentFrequency.get(term)
          if (df.nonEmpty) {
            val score = tf * scala.math.log(numberOfDocs / df.get)
            Some(term, doc, score)
          }
          else None
        })

        tfIdf.saveAsTextFile("tfidf")
        tfIdf.map(x => (x._1, (x._2, x._3)))
      }
      else {
        sc.textFile("./tfidf/part-*").map(x => x.split(',')).map(x => (x(0), (x(1), x(2).toDouble)))
      }
    }
    //tfidfRDD.top(40).foreach( x=> println("term:" + x._1+ " doc:" + x._2._1 + " score: "+ x._2._2.toString))

    while (true) {
      println()
      println("Enter a sentence or 'q' to quit")
      val input: String = Console.in.readLine()
      if (input == "q")
        System.exit(0)
      println(s"Querying...")

      val queryTokens = tokenize(input, stopWords).distinct.toSet
      //val bcTokens = sc.broadcast(queryTokens)

      val filteredTfIdf = tfidfRDD.filter(kv => queryTokens.contains(kv._1))
      /// scoreOfDocument, numberOfQueryTermsInDocument
      val scount = filteredTfIdf.map(a => a._2).reduceByKey(_+_)

      val topMatches = scount.map(kv =>  ( kv._2 * 1.0 / queryTokens.size, kv._1) ).top(10)

      if (topMatches.isEmpty)
        println("Couldn't find any relevant documents")
      else {
        println(s"Top results:")
        topMatches.foreach(println)
      }
    }


  }

  def tokenize(line : String, stopWords : Seq[String]) : Array[String] = {
    line.filter(ch => ch.isLetterOrDigit || ch == ' ').toLowerCase.split(" ").map(x => x.trim).filter(p => p != "" && !stopWords.contains(p))
  }
  /**
    * Represents a parsed Wikipedia page from the Wikipedia XML dump
    *
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    *
    * @param title Title of the current page
    * @param text Text of the current page including markup
    * @param isCategory Is the page a category page, not perfectly accurate
    * @param isFile Is the page a file page, not perfectly accurate
    * @param isTemplate Is the page a template page, not perfectly accurate
    */
  case class Page(var id:String, var title: String, var text: String)

  /**
    * A helper class that allows for a WikiArticle to be serialized and also pulled from the XML parser
    *
    * @param page The WikiArticle that is being wrapped
    */
  case class WrappedPage(var page: WikiArticle = new WikiArticle) {}

  /**
    * Helper class for parsing wiki XML, parsed pages are set in wrappedPage
    *
    */
  class SetterArticleFilter(val wrappedPage: WrappedPage) extends IArticleFilter {
    @throws(classOf[SAXException])
    def process(page: WikiArticle, siteinfo: Siteinfo)  {
      wrappedPage.page = page
    }
  }

  def readFile(filename: String): Seq[String] = {
    val bufferedSource = io.Source.fromFile(filename)
    val lines = (for (line <- bufferedSource.getLines()) yield line).toList
    bufferedSource.close
    lines
  }

  /**
    * Reads a wiki dump xml file, returning a single row for each <page>...</page>
    * https://en.wikipedia.org/wiki/Wikipedia:Database_download
    * https://meta.wikimedia.org/wiki/Data_dump_torrents#enwiki
    */
  def readWikiDump(sc: SparkContext, file: String) : RDD[(Long, String)] = {
    val conf = new Configuration()
    conf.set(XMLInputFormat.START_TAG_KEY, "<page>")
    conf.set(XMLInputFormat.END_TAG_KEY, "</page>")
    val rdd = sc.newAPIHadoopFile(file, classOf[XMLInputFormat], classOf[LongWritable], classOf[Text], conf)
    rdd.map{case (k,v) => (k.get(), new String(v.copyBytes()))}
  }


  def parsePages(rdd: RDD[(Long, String)]): RDD[(Long, Page)] = {
    rdd.mapValues{
      text => {
        val wrappedPage = new WrappedPage
        //The parser occasionally exceptions out, we ignore these
        try {
          val parser = new WikiXMLParser(new ByteArrayInputStream(text.getBytes), new SetterArticleFilter(wrappedPage))
          parser.parse()
        } catch {
          case e: Exception =>
        }
        val page = wrappedPage.page
        if (page.getText != null && page.getTitle != null
          && page.getId != null) {
          val text = page.getText
          if (text.startsWith("#REDIRECT")) None
          else Some(Page(page.getId,page.getTitle, page.getText))
        } else {
          None
        }
      }
    }.flatMapValues(_.toSeq)
  }

}
