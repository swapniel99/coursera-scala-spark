package stackoverflow

import org.scalatest.{FunSuite, BeforeAndAfterAll}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import java.io.File

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {

  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")
    override def langSpread = 50000
    override def kmeansKernels = 45
    override def kmeansEta: Double = 20.0D
    override def kmeansMaxIterations = 120

    @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("StackOverflow")
    @transient lazy val sc: SparkContext = new SparkContext(conf)

    val lines: RDD[String] = sc.parallelize(
       List("1,10,,,2,C++",
            "2,11,,10,4,",
            "2,12,,10,1,",
            "1,13,,,3,PHP",
            "2,14,,13,3,",
            "1,15,,,0,Python"))

    val raw: RDD[Posting]                                  = rawPostings(lines)
    val grouped: RDD[(Int, Iterable[(Posting, Posting)])]  = groupedPostings(raw)
    val scored: RDD[(Posting, Int)]                        = scoredPostings(grouped)
    val vectors: RDD[(Int, Int)]                           = vectorPostings(scored)

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

  override def afterAll(): Unit = {
    import testObject._
    sc.stop()
  }

  test("'rawPostings' should work.") {
    val raw = testObject.raw
    val res1 = raw.count() == 6
    val res2 = raw.first().id == 10
    assert(res1)
    assert(res2)
  }

  test("'groupedPostings' should work.") {
    val grouped = testObject.grouped
    val res1 = grouped.count() == 2
    val res2 = grouped.sortBy((tup: (Int, Iterable[(Posting, Posting)])) => tup._1).first()._2.size == 2
    assert(res1)
    assert(res2)
  }

  test("'scoredPostings' should work.") {
    val scored = testObject.scored
    val res1 = scored.count() == 2
    val res2 = scored.sortBy((tup: (Posting, Int)) => tup._2, ascending = false).first()._2 == 4
    assert(res1)
    assert(res2)
  }

  test("'vectorPostings' should work.") {
    val vectors = testObject.vectors
    val res1 = vectors.count() == 2
    val res2 = vectors.sortBy((tup: (Int, Int)) => tup._1).first() == (100000, 3)
    assert(res1)
    assert(res2)
  }

  test("'kmeans' should work.") {
    val a = Array.range(0, 45)
    val b = a.map(_ * 50000)
    val means = b zip a
    val vectors = testObject.vectors
    val newmeans = testObject.kmeans(means, vectors)
    val res1 = newmeans.length == 45
    val res2 = newmeans(2) == (100000, 3) && newmeans(5) == (250000, 4)
    assert(res1)
    assert(res2)
  }

  test("'clusterResults' should work.") {
    val a = Array.range(0, 45)
    val b = a.map(_ * 50000)
    val means = b zip a
    means(2) = (100000, 3)
    means(5) = (250000, 4)
    val vectors = testObject.vectors
    val result = testObject.clusterResults(means, vectors)
    val res1 = result(0) == ("PHP", 100.0, 1, 3)
    val res2 = result(1) == ("C++", 100.0, 1, 4)
    assert(res1)
    assert(res2)
  }

}
