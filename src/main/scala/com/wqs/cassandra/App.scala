package com.wqs.cassandra

import com.datastax.spark.connector.writer.SqlRowWriter
/**
  * Created by wqs on 16/4/23.
  */
object App {

  def main(args: Array[String]) {

    val seq = IndexedSeq[String]("zhangsan","wangwu")
    val values=("zhangsan","25","man")
    val columnSeq = IndexedSeq("name","age","sex")
    println(System.getProperty("java.io.tmpdir"))

  }


}
