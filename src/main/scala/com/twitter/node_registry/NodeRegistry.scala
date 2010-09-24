package com.twitter.node_registry

import scala.collection._
import com.twitter.json.Json
import com.twitter.zookeeper.ZooKeeperClient
import net.lag.logging.Logger
import org.apache.zookeeper.Watcher.Event.KeeperState
import org.apache.zookeeper.{CreateMode, KeeperException, WatchedEvent}

trait Serializer {
  def serialize(map: immutable.Map[String, Int]): String

  def deserialize(data: String): Map[String, Int]
}

class JsonSerializer extends Serializer {
  override def serialize(map: immutable.Map[String, Int]): String = {
    Json.build(map).toString
  }

  override def deserialize(data: String): immutable.Map[String, Int] = {
    Json.parse(data).asInstanceOf[immutable.Map[String, Int]]
  }
}

trait Store {
  /**
   * Stores a node, data should be already serialized
   */
  def registerNode(host: String, port: Int, data: String)

  /**
   * Gets a map of nodes => serialized data
   */
  def getNodes: immutable.Map[String, String]

  /**
   * Removes a node
   */
  def removeNode(host: String, port: Int)
}

class ZookeeperStore(servers: Iterable[String], sessionTimeout: Int, connectionRetryIntervalMS: Int, basePath: String) extends Store {
  private val log = Logger.get
  var zk: ZooKeeperClient = null
  private var connected = false

  connectToZookeeper()

  private def connectToZookeeper() {
    log.info("Attempting connection to Zookeeper servers %s with base path %s".format(servers, basePath))
    zk = new ZooKeeperClient(servers.mkString(","), sessionTimeout, basePath)
    zk.createPath("hosts")
  }

  override def registerNode(host: String, port: Int, data: String) {
    var created = false
    val startTime = System.currentTimeMillis()
    val timeoutMS = sessionTimeout * 2
    val nodePath = "%s:%d".format(host, port)
    while (!created && System.currentTimeMillis() < (startTime + timeoutMS)) {
      try {
        zk.create("hosts/%s".format(nodePath), data.getBytes, CreateMode.EPHEMERAL)
        created = true
      } catch {
        case _ : KeeperException.NodeExistsException => {
          log.warning("Ephemeral node " + nodePath + " already exists. Retrying...")
          Thread.sleep(1000)
        }
      }
    }
    if (!created) {
      throw new RuntimeException("Unable to create ephemeral node " + nodePath)
    }

  }

  override def getNodes: immutable.Map[String, String] = {
    zk.getChildren("hosts").foldLeft(immutable.Map[String, String]()) { (hosts, host) =>
      try {
        hosts ++ immutable.Map(host -> new String(zk.get("hosts/%s".format(host))))
      } catch {
        case e: KeeperException.NoNodeException => hosts
      }
    }
  }

  override def removeNode(host: String, port: Int) {
    zk.delete("hosts/%s:%d".format(host, port))
  }
}

/**
 * Server set takes a store and a serializer.
 * This is what most people will want to use all the time
 */
class ServerSet(store: Store, serializer: Serializer) {
  /**
   * Join adds a server to the registry. "endpoints" is a Map that will be
   * serialized and stored as the payload
   */
  def join(host: String, port: Int, endpoints: immutable.Map[String, Int]) {
    store.registerNode(host, port, serializer.serialize(endpoints))
  }

  /**
   * Remove a server from the registry
   */
  def remove(host: String, port: Int) {
    store.removeNode(host, port)
  }

  /**
   * Get a list of all nodes. Returns a map of "host:port" => deserialized data
   */
  def list: Map[String, Map[String, Int]] = {
    store.getNodes.foldLeft(immutable.Map[String, Map[String, Int]]()) { (map, tuple) =>
      val (host, data) = tuple
      map + (host -> serializer.deserialize(data))
    }
  }
}
