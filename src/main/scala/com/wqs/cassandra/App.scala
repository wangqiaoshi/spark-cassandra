package com.wqs.cassandra

import java.io.File
import java.math.BigDecimal

import com.datastax.spark.connector.cql._
import com.datastax.spark.connector.types._

import com.datastax.spark.connector.writer.{DefaultRowWriter, SqlRowWriter}
import org.apache.cassandra.config.DatabaseDescriptor
import org.apache.cassandra.streaming.StreamResultFuture
import org.apache.cassandra.utils.OutputHandler
import org.apache.spark.{SparkContext, SparkConf}

/**
  * Created by wqs on 16/4/23.
  */
object App {
  case class RowOfStrings(c1: String, c2: String, c3: String)
  def main(args: Array[String]) {
    val column1 = ColumnDef("c1", PartitionKeyColumn, IntType)
    val column2 = ColumnDef("c2", ClusteringColumn(0), DecimalType)
    val column3 = ColumnDef("c3", RegularColumn, TimestampType)
    val table = TableDef("test", "table", Seq(column1), Seq(column2), Seq(column3))
    println(table.cql)
    val rowWriter = new DefaultRowWriter[Tuple3[String,String,String]](
      table, table.columnRefs)

    val obj = ("1", "1.11", "2015-01-01 12:11:34")
    val buf = Array.ofDim[Any](3)
    rowWriter.readColumnValues(obj, buf)
    val columnNames = rowWriter.columnNames
    val converters = table.columnTypes.map(t=>t.converterToCassandra)

    val i1 = rowWriter.columnNames.indexOf("c1")
    val i2 = rowWriter.columnNames.indexOf("c2")
    val i3 = rowWriter.columnNames.indexOf("c3")
//    assert(buf(i1)==1)
    println(converters(1).convert(buf(1)))
    //    assert(buf(i2)==new BigDecimal(1.11));
//    println((buf(i1),buf(i2),buf(i3)))

    //    val seq = IndexedSeq[String]("zhangsan","wangwu")
//    val values=("zhangsan","25","man")
//    val columnSeq = IndexedSeq("name","age","sex")
//    println(System.getProperty("java.io.tmpdir"))
//    match_tuple((0,"Scala"))
//    match_tuple((2,0))
//    match_tuple((0,1,2,3,4,5))

  }
  def match_tuple(tuple : Any) = tuple match {
    case (0, _) => println("Tuple:" + "0")
    case (x, 0) => println("Tuple:" + x )
    case _ => println("something else")
  }



  def demo(): Unit ={
    val conf = new SparkConf().setAppName("sharding-hour")
    conf.set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
    val sc = new SparkContext(conf)
    sc.parallelize(0l to 100l).map(uuid=>(uuid,"zhangsan")).foreachPartition(datas=>{
      val tempFile: File = getSStableTempDir("myks", "mytable")
      val writer: CassandraSStableWriter = new CassandraSStableWriter(tempFile)

      val tableScheme: String = "create table myks.mytable(uuid bigint primary key,value text)"
      val insertStatement: String = "insert into myks.mytable(uuid,value) values(?,?)"

      val column1 = ColumnDef("uuid", PartitionKeyColumn,BigIntType)
      val column2 = ColumnDef("value", RegularColumn, TimestampType)
      val table = TableDef("myks", "mytable", Seq(column1),Nil,Seq(column2))

      val rowWriter = new DefaultRowWriter[Tuple2[Long,String]](
        table, table.columnRefs)
//      rowWriter.columnNames.foreach()
//      val app = new App()
      for(data<-datas) {
        val buf = Array.ofDim[Any](2)
        rowWriter.readColumnValues(data, buf)
        val i1 = rowWriter.columnNames.indexOf("uuid")
        val i2 = rowWriter.columnNames.indexOf("value")


//        val map1= new java.util.HashMap[String, Any]
//        map1.put("uuid", buf(i1).)
//        map1.put("value",buf(i2))


      }
//      writer.writeSStable(datas, tableScheme, insertStatement)
//      loadSStable(tempFile)
    })

  }

  @throws(classOf[Exception])
  def getSStableTempDir(keySpace: String, tableName: String): File = {
    val maxAttempt: Int = 10
    var currentAttempt: Int = 1
    var tempFile: File = null
    val rootTempDir: String = System.getProperty("java.io.tmpdir")
    while (tempFile == null) {
      if (currentAttempt > maxAttempt) {
        throw new Exception("create file failed")
      }
      val path: String = "spark-" + System.currentTimeMillis + File.separator + keySpace + File.separator + tableName
      tempFile = new File(rootTempDir + path)
      System.out.println(rootTempDir + path)
      if (!tempFile.mkdirs) tempFile = null
      currentAttempt += 1
    }
    return tempFile
  }

  def loadSStable(tempFile: File) {
    try {
//      val nodes: Array[String] = Array("192.168.6.52")
//      val options: LoaderOptions = new LoaderOptions(tempFile, nodes, 5)
//      options.setRpcPort(9161)
//      val handler: OutputHandler = new OutputHandler.SystemOutput(options.verbose, options.debug)
//      val client: CassandraLoadClient = new CassandraLoadClient(options)
//      val loader: CassandraLoader = new CassandraLoader(tempFile, client, handler, options.connectionsPerHost)
//      DatabaseDescriptor.setStreamThroughputOutboundMegabitsPerSec(options.throttle)
//      DatabaseDescriptor.setInterDCStreamThroughputOutboundMegabitsPerSec(options.interDcThrottle)
//      val indicator: ProgressIndicator = new ProgressIndicator
//      val future: StreamResultFuture = loader.stream(options.ignores, indicator)
//      indicator.printSummary(options.connectionsPerHost)
//      future.get
    }
    catch {
      case e: Exception => {
      }
    }
  }


}

class App[T](rowWriter:DefaultRowWriter[T],tableDef: TableDef){

//  def println(datas:Iterator[T]): Iterator[T] ={
//    for(data<-datas){
//      val buf = Array.ofDim[Any](tableDef.columns.size)
//      rowWriter.readColumnValues(data, buf)
//      val result=tableDef.columns.indices.map(i=>buf(i))
//      result.head
//     }
//
//
//
//
//    }
//
//  }


}



