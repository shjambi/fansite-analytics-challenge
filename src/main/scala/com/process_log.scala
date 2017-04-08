import scala.io.Source
import java.io._

import java.util.Date
import java.nio.charset.CodingErrorAction
import scala.io.Codec
import scala.io._

import com.github.nscala_time.time.Imports._
import org.joda.time.DateTime

import scalaz.stream._
import scalaz.stream.io._
import scalaz.concurrent.Task

import java.io.File
import java.io.FileInputStream



object process_log {

  case class Logline(
    val host: String, 
    val timestamp: String, 
    val request: String, 
    val resource: String, 
    val http: String, 
    val code: String, 
    val bytes: String)

  var logs = List.empty[Logline]
  val file_name = "log_input/log1.txt"

  def feature1() = {
    val most_visiting_hosts = logs.groupBy(_.host)
      .map(x => (x._1, x._2.length)).toList.sortBy(-_._2).take(10)
    // write to the output file
    val pw = new PrintWriter(new File("log_output/hosts.txt" ))
    for (l <- most_visiting_hosts) pw.write(l._1+','+l._2+'\n')
    pw.close
    println("feature 1 is completed")
  }


  def feature1_scalaz() = {
    val converter: Task[Unit] =
      io.linesR(file_name)
        .map(
          words => words.toString.filter(!"\"".contains(_)).split(" ")//.toList
          )
        .map (
          ln => if (ln.length<10) 
                  Logline(ln(0), ln(3)+' '+ln(4), ln(5), ln(6), "", ln(7), ln(8)) 
                else 
                  Logline(ln(0), ln(3)+' '+ln(4), ln(5), ln(6), ln(7), ln(8), ln(9))
        )
        .chunk(1000)
        .map(_.groupBy(_.host))
        .map(_.mapValues(x=>x.length))
        .map(_.toSeq.sortBy(-_._2))
        .map(_.take(10))
        // .map(_.flatten)
        .map(_.toString)
        .pipe(text.utf8Encode)
        .to(io.fileChunkW("log_output/hosts_scalaz.txt"))
        .run
    val u: Unit = converter.run
    println("feature 1 scalaz is completed")
  }


  def feature12() = {
    val most_bandwidth_intensive_hosts = logs.groupBy(_.host)
      .mapValues(x => x.map(_.bytes.toInt))
      .map(x => (x._1, x._2.sum)).toList.sortBy(-_._2).take(10)
    // write to the output file
    val pw = new PrintWriter(new File("log_output/hosts_bytes.txt" ))
    for (l <- most_bandwidth_intensive_hosts) pw.write(l._1+','+l._2+'\n')
    pw.close
    println("feature 1_2 is completed")
  }


  def feature2() = {
    val most_bandwidth_intensive_resources = logs.groupBy(_.resource)
      .mapValues(x => x.map(_.bytes.toInt))
      .map(x => (x._1, x._2.sum)).toList.sortBy(-_._2).take(10)
    // write to the output file
    val pw = new PrintWriter(new File("log_output/resources.txt" ))
    for (l <- most_bandwidth_intensive_resources) pw.write(l._1+','+l._2+'\n')
    pw.close
    println("feature 2 is completed")
  }


  def num_of_logs(date: String, time_period: Long): Int = {     
     val time_duration = DateTime
      .parse(date, DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]"))
      .getMillis() 
    val mylist = logs
      .map(x => x.timestamp)
      .filter(x => DateTime.parse(x,DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]")).getMillis() >= time_duration &&
        DateTime.parse(x,DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]")).getMillis() <= (time_duration + time_period)
      )
    mylist.count(_.isInstanceOf[String])
  }


  def feature3() = {
    val most_visiting_hours = logs.
      map(x => (x.timestamp, num_of_logs(x.timestamp, 3600000))).toList.sortBy(-_._2).take(10)
    // write to the output file
    val pw = new PrintWriter(new File("log_output/hours.txt" ))
    for (l <- most_visiting_hours) pw.write(l._1+','+l._2.toString+'\n')
    pw.close
    println("feature 3 is completed")
 
  }


 def logs_by_host(host: String, date: String, time_period: Long): List[Logline] = {     
     val start_time = DateTime
      .parse(date, DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]"))
      .getMillis() 
    val mylist = logs
      .filter(x => x.host == host &&
        DateTime.parse(x.timestamp,DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]")).getMillis() > start_time &&
        DateTime.parse(x.timestamp,DateTimeFormat.forPattern("[dd/MMM/YYYY:HH:mm:ss -0400]")).getMillis() <= (start_time + time_period)
      )
    mylist
  }


  def feature4() = {
    val blocked = logs
      .map(x => (x.host, logs_by_host(x.host, x.timestamp, 20000).take(3)))
      .map(x => x._2)
      // if we found three logs for the current host over 20 sec, 
      // check if three consecutive failed logs are found
      .filter(_.size == 3) 
      .filter(x => (x(0).code == "304")&&(x(1).code == "304")&&(x(2).code == "304"))
      // list all following accesses within the next 5 min as blocked 
      .map(x => logs_by_host(x(2).host, x(2).timestamp, 300000))
      .flatten
      .distinct
      // write to the output file
      val pw = new PrintWriter(new File("log_output/blocked.txt" ))
      for (l <- blocked) 
        pw.write(l.host+" - - "+l.timestamp+" \""+l.request+" "+ l.resource+" "+l.http+"\" "+l.code+" "+l.bytes+'\n')
      pw.close
      println("feature 4 is completed")
  }


  def main(args: Array[String]) {

    println("Reading log info ...")
    val decoder = Codec.UTF8.decoder.onMalformedInput(CodingErrorAction.IGNORE)
    for (line <- Source.fromFile(file_name)(decoder).getLines()) {
      val ln = line.filter(!"\"".contains(_)).split(" ")
      // 0=pm110.spectra.net
      // 1=-
      // 2=-
      // 3=[01/Jul/1995:00:09:39
      // 4=-0400]
      // 5=GET
      // 6=/images/USAlogosmall.gif
      // 7=HTTP/1.0
      // 8=304
      // 9=1420
      if (ln(ln.length-1) == "-") ln(ln.length-1) = "0"
      if (ln.length<10){ 
        val info = Logline(ln(0), ln(3)+' '+ln(4), ln(5), ln(6), "", ln(7), ln(8))
        logs = logs :+ info 
      }
      else{
        val info = Logline(ln(0), ln(3)+' '+ln(4), ln(5), ln(6), ln(7), ln(8), ln(9))
        logs = logs :+ info 
      }

    }
    println("Running features ...")
    feature1()
    feature12()
    feature1_scalaz()
    feature2()
    feature3()
    feature4()

  }

}