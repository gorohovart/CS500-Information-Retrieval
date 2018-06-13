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

object WikiSearch {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
      .set("spark.executor.memory", "10g")
      .set("spark.driver.maxResultSize", "10g")

    val sc = new SparkContext( conf )
    val stopWords = readFile("stopWords.txt")
    val parse = true
    val compute = true

    val tfidfRDD : RDD[(String, (String, Double))] = {
      if (compute)
        tfIDFcompute(sc,compute, stopWords)
      else
        tfIDFload(sc)
    }

    /// Query processing
    while (true) {
      println()
      println("Enter a sentence or 'q' to quit")
      val input: String = Console.in.readLine()
      if (input == "q")
        System.exit(0)
      println(s"Querying...")

      val queryTokens = tokenize(input, stopWords).distinct.toSet

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

  def tfIDFload(sc:SparkContext) = {
    sc.textFile("./tfidf/part-*").map(x => x.split(',')).map(x => (x(0), (x(1), x(2).toDouble)))
  }

  def tfIDFcompute(sc:SparkContext, needToParse : Boolean, stopWords: Seq[String]) = {
    val pages: RDD[(String, String)] =
      if (needToParse) {
        val rawpages = readWikiDump(sc, "ruwiki.xml")
        val pages = parsePages(rawpages)
                    .values
                    .map(p => (p.title, p.text))
        val pagesOnlyWords = pagesToWords(pages, stopWords)
        pagesOnlyWords.saveAsTextFile("pages")
        pagesOnlyWords
      }
      else {
        sc.textFile("pages/part-*")
          .map(x => {
            val y = x.split(',')
            (y(0), y(1))
          })
      }

    val numberOfDocs = pages.count()

    val termFrequency =
      pages
        .flatMap(y => y._2.split(" ")
                          .map(s => ((s, y._1), 1))
                )
        .reduceByKey(_+_)

    val documentFrequency =
      termFrequency
        .map(x => (x._1._1, 1))
        .reduceByKey(_+_)
        .collectAsMap()

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

  def pagesToWords(pages: RDD[(String, String)], stopWords : Seq[String]) = {
    pages.map(x => {
      val name = x._1.filter(x => x != ',')
      (name, tokenize(name + x._2, stopWords).mkString(" "))
    })
  }

  def tokenize(line : String, stopWords : Seq[String]) : Array[String] = {
    line.map(ch => if (ch.isLetterOrDigit) {ch} else {' '})
        .split(" ")
        .filter(p => p != "" && !stopWords.contains(p))
  }

  case class Page(var id:String, var title: String, var text: String)
  case class WrappedPage(var page: WikiArticle = new WikiArticle) {}
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
          val text = page.getText.toLowerCase
          if (text.startsWith("#redirect") || text.startsWith("#перенаправление")) None
          else Some(Page(page.getId,page.getTitle, page.getText))
        } else {
          None
        }
      }
    }.flatMapValues(_.toSeq)
  }

}
