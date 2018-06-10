package cs500ir.spark

import java.io.ByteArrayInputStream

import org.apache.spark.sql.SparkSession
import info.bliki.wiki.dump.{IArticleFilter, Siteinfo, WikiArticle, WikiXMLParser}
import info.bliki.wiki.filter.WikipediaParser
import info.bliki.wiki.model.WikiModel
import io.mindfulmachines.input.XMLInputFormat
import org.apache.commons.lang3.StringEscapeUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.htmlcleaner.HtmlCleaner
import org.xml.sax.SAXException

object HelloSpark {
  def main(args: Array[String]): Unit = {
    //val spark = SparkSession
    //  .builder()
    //  .appName("Spark SQL basic example")
    //  .config("spark.some.config.option", "some-value")
    //  .getOrCreate()

    val conf = new SparkConf().setAppName( "SparkTest" ).setMaster("local[*]" )
      .set("spark.executor.memory", "1g")

    val sc    = new SparkContext( conf )
    //val qwe = new java.io.File(".").getCanonicalPath
    //val path = "file://" +"wikidump.xml".getPath
    val rawpages = readWikiDump(sc, "ruwiki-20180420-pages-articles-multistream.xml")

    println(parsePages(rawpages).count().toString())
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
  case class Page(title: String, text: String, isCategory: Boolean , isFile: Boolean, isTemplate: Boolean)

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
          && page.getId != null && page.getRevisionId != null
          && page.getTimeStamp != null) {
          Some(Page(page.getTitle, page.getText, page.isCategory, page.isFile, page.isTemplate))
        } else {
          None
        }
      }
    }.flatMapValues(_.toSeq)
  }

}
