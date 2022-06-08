package stackoverflow

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext.*
import org.apache.spark.rdd.RDD
import stackoverflow.Aliases.Answer
import stackoverflow.StackOverflow.{getClass, sc}
import stackoverflow.StackOverflowSuite.getClass

import java.io.File
import scala.concurrent.duration.Duration
import scala.io.{Codec, Source}
import scala.util.Properties.isWin

object StackOverflowSuite:
  val conf: SparkConf = new SparkConf().setMaster("local[2]").setAppName("StackOverflow")
  val sc: SparkContext = new SparkContext(conf)

class StackOverflowSuite extends munit.FunSuite:
  import StackOverflowSuite.*

  lazy val testObject: StackOverflow = new StackOverflow {
    override val langs: List[String] =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy"
      )
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("groupedPostings works") {
    val postings = StackOverflowSuite.sc.parallelize(List(
      Posting(postingType = 1, id = 1, acceptedAnswer = Some(4), parentId = None, score = 3, tags = None),
      Posting(postingType = 1, id = 2, acceptedAnswer = Some(5), parentId = None, score = 6, tags = None),
      Posting(postingType = 1, id = 3, acceptedAnswer = None, parentId = None, score = 9, tags = None),
      Posting(postingType = 2, id = 4, acceptedAnswer = None, parentId = Some(1), score = 10, tags = None),
      Posting(postingType = 2, id = 5, acceptedAnswer = None, parentId = Some(2), score = 20, tags = None)
    ))

    val groupedPostings = testObject.groupedPostings(postings)
    val grouped = groupedPostings.collect()

    assert(grouped.length == 2)

    val posting1 = grouped.find(_._1 == 1)
    assert(posting1.nonEmpty)

    val answer4 = grouped.find(_._1 == 1).flatMap(_._2.headOption.map(_._2))
    assert(answer4.nonEmpty)
    assert(answer4.get.id == 4)
    assert(answer4.get.parentId.contains(1))
  }

  test("scoredPostings works") {
    val highScore = 100
    val postings = StackOverflowSuite.sc.parallelize(List(
      Posting(postingType = 1, id = 1, acceptedAnswer = Some(4), parentId = None, score = 3, tags = None),
      Posting(postingType = 2, id = 4, acceptedAnswer = None, parentId = Some(1), score = 10, tags = None),
      Posting(postingType = 2, id = 5, acceptedAnswer = None, parentId = Some(1), score = highScore, tags = None)
    ))

    val groupedPostings = testObject.groupedPostings(postings)
    val scoredPostings = testObject.scoredPostings(groupedPostings).collect()

    assert(scoredPostings.length == 1)
    assert(scoredPostings.head._2 == highScore)
  }

  test("vectorPostings works") {
    val postings = StackOverflowSuite.sc.parallelize(List(
      Posting(postingType = 1, id = 1, acceptedAnswer = Some(4), parentId = None, score = 3, tags = Some("Scala")),
      Posting(postingType = 2, id = 4, acceptedAnswer = None, parentId = Some(1), score = 10, tags = Some("Scala")),
      Posting(postingType = 2, id = 5, acceptedAnswer = None, parentId = Some(1), score = 100, tags = Some("Java"))
    ))

    val groupedPostings = testObject.groupedPostings(postings)
    val scoredPostings = testObject.scoredPostings(groupedPostings)
    val vectorPostings = testObject.vectorPostings(scoredPostings).collect()

    assert(vectorPostings.nonEmpty)
    assert(vectorPostings.head._1 == 10 * 50000) // Scala's language id * lang spread
  }

  import scala.concurrent.duration.given
  override val munitTimeout: Duration = 300.seconds
