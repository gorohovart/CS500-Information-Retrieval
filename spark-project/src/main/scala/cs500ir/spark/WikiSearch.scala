package cs500ir.spark

import java.io.{BufferedWriter, ByteArrayInputStream, File, FileWriter}

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
import org.apache.spark.mllib.feature.Stemmer
import org.apache.lucene.analysis.snowball
import info.bliki.api.XMLPagesParser
import org.apache.spark.ml.feature.{HashingTF, IDF}

import scala.io


object WikiSearch {
  var stopWords : Set[String] = readFile("stopWords.txt").toSet

  def main(args: Array[String]): Unit = {

    //val sparkSession = SparkSession
    //  .builder()
     // .appName("WikiSearch")
     // .config("spark.master", "local")
     // .getOrCreate()

    //val conf = sparkSession.conf

    //conf.set("spark.executor.memory", "5g")
    //conf.set("spark.driver.maxResultSize", "5g")
    //conf.set("spark.driver.memory", "5g")
    val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
      .set("spark.executor.memory", "3g")
      .set("spark.driver.maxResultSize", "3g")

    val sc = new SparkContext( conf ) //sparkSession.sparkContext//

    val parse = false
    val compute = false
    val tfidf = false

    if (compute)
      tfIDFcompute(sc,parse)

    if (tfidf) {
      val tfidfRDD: RDD[(String, (String, Int))] = tfIDFload(sc)

      val tfIdfIntermed = tfidfRDD.aggregateByKey(List.empty[(String, Int)])(
        (x, y) => x :+ y,
        (x, y) => x ++ y
      ).mapValues(docScoreList => docScoreList.sortBy { case (_, score) => -1 * score }.take(10))

      tfIdfIntermed.saveAsObjectFile("tfIDFfiltered")
    }

    val tfIdfLoaded = sc.objectFile[(String, List[(String, Int)])]("tfIDFfiltered/part-*")
    val tfIdfMap = tfIdfLoaded.collectAsMap()

    /// Query processing
    while (true) {
      println()
      println("Enter a sentence or 'q' to quit")
      val input: String = Console.in.readLine()
      if (input == "q")
        System.exit(0)
      println(s"Querying...")

      val queryTokens = tokenize(input).distinct

      val topMatches =
        queryTokens.flatMap { x =>
          val res = tfIdfMap.get(x)
          if (res.isDefined)
            Some(res.get)
          else
            None
        }.flatten
          .groupBy{case (doc,_) => doc}
            .map{ case (doc, scores) => (doc, scores.map(x => x._2).sum)}
            .toSeq
            .sortBy{case (_,s) => -1 * s}.take(100)

      if (topMatches.isEmpty)
        println("Couldn't find any relevant documents")
      else {
        println(s"Top results:")
        topMatches.foreach(println)
      }

    }
  }

  def tfIDFload(sc:SparkContext) = {
    sc.objectFile[(String, (String, Int))]("./tfidf/part-*")//.map(x => x.filter(x => x != ')' && x != '(').split(',')).map(x => (x(0), (x(1), x(2).toInt)))
  }

  def getPages(sc:SparkContext, needToParse : Boolean) : RDD[(String, Seq[String])] = {
    if (needToParse) {
      val rawpages = readWikiDump(sc, "ruwiki.xml")
      val pages = parsePages(rawpages)
      val pagesOnlyWords = pagesToWords(pages)
      pagesOnlyWords.saveAsTextFile("pages")
    }
    sc.textFile("pages/part-*")
      .map(x => {
        val y = x.split(',')
        (y(0), y(1).split(" "))
      })
  }

  def tfIDFcompute(sc:SparkContext, needToParse : Boolean) = {
    val pages = getPages(sc,needToParse)

    val numberOfDocs = pages.count()

    // сколько раз встречается слово в документе
    val termFrequency =
      pages
        .flatMap { case (docName, content) =>
          content.groupBy(x => x)
                 .flatMap{ case (word, words) =>
                   val tf = words.length
                   if (tf > 1)
                     Some((word, docName, words.length))
                   else
                     None}}

    // сколько слово встречается во всём корпусе
    val documentFrequency =
      termFrequency
        .map{ case (word,_,_) => (word, 1)}
        .reduceByKey(_+_)
        .collectAsMap()

    termFrequency.flatMap{ case (term,doc,tf) => {
      val df = documentFrequency.get(term)
      if (df.nonEmpty) {
        val score = tf * scala.math.log(numberOfDocs / df.get) // numberOfWordsInDoc(doc)
        Some(term, (doc, score.toInt))
      }
      else None
    }}.saveAsTextFile("tfidf")
  }

  def pagesToWords(pages: RDD[(String, String)]) = {
    pages.map(x => {
      val name = x._1.filter(x => x != ',')
      (name, tokenize(name + x._2).mkString(" "))
    })
  }

  def tokenize(line : String) : Array[String] = {
    line.map(ch => if (ch.isLetterOrDigit) {ch} else {' '})
      .split(" ")
      .filter(p => p != "" && !stopWords.contains(p) && p.length > 2)
  }

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

  def parsePages(rdd: RDD[(Long, String)]): RDD[(String, String)] = {
    rdd.flatMap( t => {
      val text = t._2
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
        else Some((page.getTitle, text))
      } else {
        None
      }
    }
    )
  }


}
