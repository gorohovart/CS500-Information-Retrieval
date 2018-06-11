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

import scala.io.Source
import org.apache.spark.ml.feature.{HashingTF, IDF, Tokenizer}

object HelloSpark {
  def main(args: Array[String]): Unit = {
    //val sparkSession = SparkSession
    //  .builder()
    //  .appName("HelloSpark")
    //  .config("spark.master", "local")
    //  .getOrCreate()

    val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
      .set("spark.executor.memory", "2g")

    val sc    = new SparkContext( conf )
    //import sc.implicits._
    //val qwe = new java.io.File(".").getCanonicalPath
    //val path = "file://" +"wikidump.xml".getPath
    val stopWords = Source.fromFile("stopWords.txt").getLines()
    val rawpages = readWikiDump(sc, "ruwiki-20180420-pages-articles-multistream.xml")
    //rawpages.saveAsTextFile("rubackup")

    val pages = parsePages(rawpages)
    //val filteredPages =
    //  pages.values.map(y =>
    //    (y.id,
    //      (y.title,
    //       y.text.split(" ").filter(p=> p != "" && !stopWords.contains(p)))))

    val numberOfDocs = pages.count()

    val filteredPages =
      pages.values.flatMap(y => tokenize(y.text, stopWords).map( s => (y.title,s)))//.toDF()

    val termFrequency = filteredPages.countByValue()

    val documentToTerm = filteredPages.distinct().map( x => (x._2,x._1))
    val documentFrequency = documentToTerm.countByKey()

    val tfIdf = termFrequency.flatMap( x => {
      val doc = x._1._1
      val term = x._1._2
      val tf = x._2
      val df = documentFrequency.get(term)
      if (df.isDefined && df.get > 0){
        val score = tf * scala.math.log(numberOfDocs / df.get)
        Some(term, (doc,score))
      }
      else None
    })

    val tfidfRDD = sc.parallelize(tfIdf.toSeq)

    /*val dataframe = sparkSession.createDataFrame(filteredPages).toDF("id", "title", "words")

    val hashingTF = new HashingTF()
      .setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(20)

    val featurizedData = hashingTF.transform(dataframe)
    // alternatively, CountVectorizer can also be used to get term frequency vectors

    val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
    val idfModel = idf.fit(featurizedData)

    val rescaledData = idfModel.transform(featurizedData)
    //rescaledData.select("label", "features").show()
*/
    while (true) {
      println()
      println("Enter a sentence or 'q' to quit")
      val input: String = Console.in.readLine()
      if (input == "q")
        System.exit(0)
      println(s"Querying...")

      val tokens = sc.parallelize(tokenize(input, stopWords)).map(x => (x,1)).collectAsMap()
      val bcTokens = sc.broadcast(tokens)

      val joinedTfIdf = tfidfRDD.map(kv => {
        val term = kv._1
        val documentScore = kv._2
        val value = bcTokens.value.get(kv._1)
        val res =
          if (value.isDefined)
            value.get
          else
          -1
        (term, res, documentScore)
      } ).filter(kvs => kvs._2 != -1 )

      //compute the score using aggregateByKey
      val scount = joinedTfIdf.map(a =>  a._3).aggregateByKey((0.0,0))(
        ((acc, v) => (acc._1 + v, acc._2 + 1)),
        ((acc1,acc2) =>  (acc1._1 + acc2._1, acc1._2 + acc2._2) ))

      val topMatches = scount.map(kv =>  ( kv._2._1 * kv._2._2 / tokens.size, kv._1) ).top(10)
      //val redusedData = rescaledData.filter($"")


      //val topMatches =
      if (topMatches.isEmpty)
        println("Couldn't find any relevant documents")
      else {
        println(s"Top results:")
        topMatches.foreach(println)
      }
    }


  }

  def tokenize(line : String, stopWords : Iterator[String]) : Array[String] = {
    line.split(" ").filter(p => p != "" && !stopWords.contains(p))
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
          Some(Page(page.getId,page.getTitle, page.getText))
        } else {
          None
        }
      }
    }.flatMapValues(_.toSeq)
  }

}
