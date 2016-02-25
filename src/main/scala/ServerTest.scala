import java.util
import java.util.Map.Entry

import com.google.common.io.Files
import org.apache.accumulo.core.client._
import org.apache.accumulo.core.client.impl.{TabletLocator, TimeoutTabletLocator}
import org.apache.accumulo.core.client.lexicoder.IntegerLexicoder
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.accumulo.core.conf.Property
import org.apache.accumulo.core.data.{Range => AccumuloRange, _}
import org.apache.accumulo.core.iterators.user.{GrepIterator, RowDeletingIterator}
import org.apache.accumulo.core.metadata.MetadataTable
import org.apache.accumulo.core.security.Authorizations
import org.apache.accumulo.minicluster.impl.{MiniAccumuloClusterImpl, MiniAccumuloConfigImpl}
import org.apache.accumulo.minicluster.{MiniAccumuloCluster, MiniAccumuloConfig}
import org.apache.accumulo.server.replication.proto.Replication.Status
import org.apache.accumulo.shell.Shell
import org.apache.hadoop.io.Text
import org.apache.log4j.{Level, Logger}
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._
import scala.io.StdIn

/**
 * Created by bill on 1/11/16.
 */

object ReplicationTest {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.zookeeper").setLevel(Level.OFF)
    val log = LoggerFactory.getLogger(getClass)

    // Instance A
    val work_A = Files.createTempDir()
    log.info("A dir: " + work_A)
    val conf_A = new MiniAccumuloConfigImpl(work_A, "secret")
    conf_A.setInstanceName("A")
    conf_A.setNumTservers(1)
    conf_A.setProperty(Property.REPLICATION_NAME, "A")
    conf_A.setProperty(Property.INSTANCE_ZK_TIMEOUT, "10s")
    conf_A.setProperty(Property.MASTER_REPLICATION_SCAN_INTERVAL, "1s")
    conf_A.setProperty(Property.REPLICATION_WORK_ASSIGNMENT_SLEEP, "1s")
    conf_A.setProperty(Property.TSERV_WALOG_MAX_SIZE, "1M")
    conf_A.setProperty(Property.GC_CYCLE_START, "1s")
    conf_A.setProperty(Property.GC_CYCLE_DELAY, "0")
    conf_A.setProperty(Property.REPLICATION_WORK_PROCESSOR_DELAY, "1s")
    conf_A.setProperty(Property.REPLICATION_WORK_PROCESSOR_PERIOD, "1s")
    conf_A.setProperty(Property.TSERV_TOTAL_MUTATION_QUEUE_MAX, "1M")
    conf_A.setProperty(Property.TSERV_WAL_REPLICATION, 1.toString)
    conf_A.setZooKeeperPort(7654)

    // Instance B
    val work_B = Files.createTempDir()
    log.info("B Dir: " + work_B)
    val conf_B = new MiniAccumuloConfigImpl(work_B, "secret")
    conf_B.setZooKeeperPort(7655)
    conf_B.setInstanceName("B")
    conf_B.setProperty(Property.REPLICATION_NAME, "B")
    conf_B.setNumTservers(1)

    /*
    replication.peer.peer1=org.apache.accumulo.tserver.replication.AccumuloReplicaSystem,accumulo_peer,10.0.0.1,10.0.2.1,10.0.3.1
     */
    val accumulo_A = new MiniAccumuloClusterImpl(conf_A)
    val accumulo_B = new MiniAccumuloClusterImpl(conf_B)
    try {
      accumulo_A.start()
      accumulo_B.start()

      val con_A = accumulo_A.getConnector("root", new PasswordToken("secret"))

      // configure replication on A
      con_A.instanceOperations().setProperty("replication.peer.peer1",
        s"org.apache.accumulo.tserver.replication.AccumuloReplicaSystem,${accumulo_B.getInstanceName},${accumulo_B.getZooKeepers}")
      con_A.instanceOperations().setProperty("replication.peer.user.peer1", "root")
      con_A.instanceOperations().setProperty("replication.peer.password.peer1", "secret")


      // make the target table in B and get is id
      val con_B = accumulo_B.getConnector("root", new PasswordToken("secret"))
      con_B.tableOperations().create("t")
      val peer_table_id = con_B.tableOperations().tableIdMap().get("t")


      con_A.tableOperations().create("t")
      con_A.tableOperations().setProperty("t", Property.TABLE_REPLICATION.getKey, "true")
      con_A.tableOperations().setProperty("t", Property.TABLE_REPLICATION_TARGET.getKey + "peer1", peer_table_id.toString)

      println("Waiting for replication table come online...")
      con_A.tableOperations().online("accumulo.replication", true)
      println("It's online")
      
      // create "t" on A and give it data
      val bw = con_A.createBatchWriter("t", new BatchWriterConfig)
      val m = new Mutation("r")
      for(i <- 1 to 500000) {
        m.put(i.toString, "", "")
      }
      bw.addMutation(m)
      bw.close()

      var scan = con_A.createScanner("accumulo.metadata", Authorizations.EMPTY)
      println("A:META")
      scan.setRange(new AccumuloRange())
      scan.foreach(println)


      while(true) {
//        scan = con_B.createScanner("t", Authorizations.EMPTY)
        println("A:REPL")
        scan = con_A.createScanner("accumulo.replication", Authorizations.EMPTY)
        scan.setRange(new AccumuloRange())
        scan.foreach((e: Entry[Key, Value]) => {
          val s = Status.parseFrom(e.getValue.get())
          println(e.getKey.getRow)
          println(s)
        })

        scan = con_A.createScanner("accumulo.metadata", Authorizations.EMPTY)
        scan.setRange(new AccumuloRange)
        scan.foreach((e: Entry[Key, Value]) => {println(e.getKey.getRow)})
        Thread.sleep(500)

        scan = con_B.createScanner("t", Authorizations.EMPTY)
        scan.setRange(new AccumuloRange)
        scan.foreach(println)
        Thread.sleep(500)
      }

    } finally {
      accumulo_A.stop()
      accumulo_B.stop()
    }
  }
}

//object DeleteTest {
//  def main(args: Array[String]): Unit = {
//    val cxn = new ZooKeeperInstance("miniInstance", "localhost:2978").getConnector("root", "secret")
//    if(cxn.tableOperations().exists("delete_test")) cxn.tableOperations().delete("delete_test")
//    cxn.tableOperations().create("delete_test")
//    val bw = cxn.createBatchWriter("delete_test", new BatchWriterConfig)
//    def write(m: Mutation) = {
//      bw.addMutation(m)
//      bw.flush()
//    }
//    val s = cxn.createScanner("delete_test", Authorizations.EMPTY)
//    def read() = {
//      s.setRange(new AccumuloRange())
//      s.foreach(println)
//    }
//    val insert = new Mutation("A")
//    insert.put("", "", "")
//    write(insert)
//    read()
//    val delete = new Mutation("A")
//    delete.putDelete("", "")
//    write(delete)
//    read()
//    val reinsert = new Mutation("A")
//    reinsert.put("", "", "")
//    write(reinsert)
//    read()
//    cxn.tableOperations().delete("delete_test")
//  }
//}
//
//object AccumuloInstance {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.OFF)
//    val log = LoggerFactory.getLogger(getClass)
//    val work_dir = Files.createTempDir()
//    log.info("Working dir: {}", work_dir)
//    val accumulo = new MiniAccumuloCluster(new MiniAccumuloConfig(Files.createTempDir(), "secret"))
//    try {
//      accumulo.start()
//      val connector = accumulo.getConnector("root", "secret")
//      connector.tableOperations().create("test")
//      write(connector.createBatchWriter("test", new BatchWriterConfig))
//      val splits = new java.util.TreeSet[Text]
//      splits.add(new Text(64.toString))
//      splits.add(new Text(128.toString))
//      splits.add(new Text(192.toString))
//      connector.tableOperations().addSplits("test", splits)
//      val msg =
//        s"""Mini Accumulo has started!
//           |Instance: ${accumulo.getInstanceName}
//           |ZooKeepers: ${accumulo.getZooKeepers}
//           |Root Password: secret
//            """.stripMargin
//      println(msg)
//      StdIn.readLine("exit? ")
//    } finally {
//      accumulo.stop()
//      work_dir.deleteOnExit()
//    }
//  }
//
//  def write(bw: BatchWriter) = {
//    import scala.language.implicitConversions
//    implicit def i2a(i: Int): String = i.toString
//    val enc = new IntegerLexicoder
//    for(i <- 1 to 256) {
//      val m = new Mutation(enc.encode(i))
//      m.put("", "", "")
//      bw.addMutation(m)
//    }
//    bw.close()
//  }
//}
//
//object MiniShell {
//  def main(args: Array[String]): Unit = {
//    Logger.getLogger("org.apache.zookeeper").setLevel(Level.OFF)
//    val log = LoggerFactory.getLogger(getClass)
//    val work_dir = Files.createTempDir()
//    log.info("Working dir: {}", work_dir)
//    val password = "secret"
//    val accumulo = new MiniAccumuloCluster(new MiniAccumuloConfig(Files.createTempDir(), password))
//    var returnCode: Option[Int] = None
//    try {
//      accumulo.start()
//      val shell = new Shell();
//      val shell_args = s"-u root -p $password -zi ${accumulo.getInstanceName} -zh ${accumulo.getZooKeepers}".split(" ")
//
//      try {
//        if (!shell.config(shell_args:_*)) {
//          returnCode = Option(shell.getExitCode)
//        } else {
//          returnCode = Option(shell.start())
//        }
//      } finally {
//        shell.shutdown
//      }
//    } finally {
//      accumulo.stop()
//      work_dir.deleteOnExit()
//    }
//    System.exit(returnCode.get)
//  }
//}
//
//object ServerTest {
//  def main(args: Array[String]): Unit = {
//    val connector = new ZooKeeperInstance(ClientConfiguration.loadDefault().withInstance("miniInstance").withZkHosts("localhost:12936")).getConnector("root", new PasswordToken("secret"))
//    import Tablets._
//    val tablets = tabletsOnServer(connector, "Williams-Mac", "test")
//    rangesForTablets(tablets.toIndexedSeq).foreach(println)
//  }
//}
//
//object ReadTables {
//  def main(args: Array[String]): Unit = {
//    val inst_name = args(0)
//    val zk = args(1)
//    val user = args(2)
//    val pass = args(3)
//    val table = args(4)
//    val server = args(5)
//    val auths = args(6)
//
//    val inst = new ZooKeeperInstance(inst_name, zk)
//    val connector = inst.getConnector(user, new PasswordToken(pass))
//
//    val locator = new TimeoutTabletLocator(TabletLocator.getLocator(inst, new Text(table)), 5)
//    locator.invalidateCache()
//    val c = new Credentials(user, new PasswordToken(pass))
//    val desired_range = new util.ArrayList[AccumuloRange]()
//    desired_range.add(new AccumuloRange)
//    val m = new util.HashMap[String, util.Map[KeyExtent, util.List[AccumuloRange]]]()
//    val f = locator.binRanges(c, desired_range, m)
////    println(f)
//    println(locator.binRanges(c, desired_range, m))
//    println(m.keys)
////    val tablets = Tablets.tabletsOnServer(connector, server, table)
////    println(s"Founds ${tablets.length} tablets")
////    val ranges = Tablets.rangesForTablets(tablets)
////    println(s"Generated ${ranges.length} ranges for $table on $server")
////
////    var scan_auths: Authorizations = null
////    if(auths.isEmpty) scan_auths = Authorizations.EMPTY
////    else scan_auths = new Authorizations(auths.split(","):_*)
////
////    val bs = connector.createBatchScanner(table, scan_auths, 8)
////    bs.setRanges(ranges)
////    var l = 0L
////    for(e <- bs) {
////      l = l + 1
////    }
////    println(s"Read total of $l entries")
//  }
//}
//
//// TODO we need to handle the last tablet
//object Tablets {
//  def tabletsOnServer(connector: Connector, server: String, table: String) = {
//    val table_id = connector.tableOperations().tableIdMap()(table)
//    if(table_id != null) {
//      val scanner = connector.createScanner(MetadataTable.NAME, Authorizations.EMPTY)
//      scanner.setRange(new AccumuloRange(new Key(table_id), new Key(table_id + "\u00ff")))
//      scanner.fetchColumn(new Text("srv"), new Text("lock"))
//      val itr = new IteratorSetting(50, classOf[GrepIterator])
//      GrepIterator.setTerm(itr, server)
//      scanner.addScanIterator(itr)
//      // parses start row value for the tablet from the metadata row value
//      scanner.toIndexedSeq.map(e => {
//        val tablet_id = e.getKey.getRow.toString
//        println(s"Tablet ID ${tablet_id}")
//        // last tablet
//        if(tablet_id.length() != table_id.length + 1) {
//          val substr_start = table_id.getBytes.length + 1 // the +1 is for the delimiting ';'
//          tablet_id.substring(substr_start)
//        } else {
//          ""
//        }
//      }).filter(!_.isEmpty)
//    } else {
//      throw new IllegalArgumentException(s"Table $table does not exist")
//    }
//  }
//
//  def rangesForTablets(tablets: IndexedSeq[String]) = {
//    var i = 0
//    val sink = new util.ArrayList[AccumuloRange]()
//    var start = new Key("\u0000")
//    for(i <- 0 to tablets.length - 1) {
//      val end = new Key(tablets(i))
//      sink.add(new AccumuloRange(start, true, end, false))
//      start = end
//    }
//    sink.add(new AccumuloRange(start, null))
//    sink
//  }
//}
//
//object RowDeletingIteratorTest {
//  def main(args: Array[String]): Unit = {
//    val cxn = new ZooKeeperInstance("miniInstance", "localhost:2978").getConnector("root", "secret")
//    if (cxn.tableOperations().exists("row_del_itr_test")) cxn.tableOperations().delete("row_del_itr_test")
//    cxn.tableOperations().create("row_del_itr_test")
//    val row_del_itr_cfg = new IteratorSetting(11, classOf[RowDeletingIterator])
//    cxn.tableOperations().attachIterator("row_del_itr_test", row_del_itr_cfg)
//
//    val bw = cxn.createBatchWriter("row_del_itr_test", new BatchWriterConfig)
//    val m = new Mutation("A")
//    for(i <- 1 to 3) m.put("", i.toString, "")
//    bw.addMutation(m)
//    bw.flush()
//
//    val s = cxn.createScanner("row_del_itr_test", Authorizations.EMPTY)
//    def read() = {
//      s.setRange(new AccumuloRange())
//      s.foreach(println)
//    }
//
//    read()
//
//    println("Read whole row. Inserting delete...")
//
//    val del = new Mutation("A")
//    del.put("", "", RowDeletingIterator.DELETE_ROW_VALUE)
//    bw.addMutation(del)
//    bw.flush()
//
//    println("Verifying row was deleted. Should see nothing.")
//    read()
//    println("Did you see anything?")
//
//    println("Adding new row data...")
//    val reinsert = new Mutation("A")
//    for(i <- 4 to 6) reinsert.put("", i.toString, "")
//    bw.addMutation(reinsert)
//    bw.flush()
//
//    println("Reading updated row...")
//    read()
//    println("Did you see anything?")
//  }
//}
