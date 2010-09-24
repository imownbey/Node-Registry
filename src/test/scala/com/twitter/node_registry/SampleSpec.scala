package com.twitter.node_registry

import org.specs._
import scala.collection._

class TestStore extends Store {
  val nodes: mutable.Map[String, String] = mutable.Map()
  override def registerNode(host: String, port: Int, data: String) {
    nodes.synchronized {
      nodes + ("%s:%d".format(host, port) -> data)
    }
  }

  override def getNodes = immutable.Map() ++ nodes

  override def removeNode(host: String, port: Int) {
    nodes.synchronized {
      nodes.removeKey("%s:%d".format(host, port))
    }
  }
}


object NodeRegistrySpec extends Specification {
  "TestStore" should {
    "track a host" in {
      val store = new TestStore
      store.registerNode("somehost.domain.com", 1234, "data")
      store.getNodes must haveKey("somehost.domain.com:1234")
      store.removeNode("somehost.domain.com", 1234)
      store.getNodes mustNot haveKey("somehost.domain.com:1234")
    }
  }

  "JsonSerializer" should {
    "serialize into JSON" in {
      val serializer = new JsonSerializer
      serializer.serialize(immutable.Map("admin" -> 1234)) mustEqual """{"admin":1234}"""
    }
  }

  "ZookeeperStore" should {
    "track a host" in {
      val store = new ZookeeperStore(Set("localhost:2181"), 10000, 10000, "/aCompany/aService/")
      store.registerNode("somehost.domain.com", 1234, "data")
      store.getNodes must haveKey("somehost.domain.com:1234")
      store.removeNode("somehost.domain.com", 1234)
      store.getNodes mustNot haveKey("somehost.domain.com:1234")
    }
  }

  "ServerSet" should {
    "use JSON and Zk" in {
      val store = new ZookeeperStore(Set("localhost:2181"), 10000, 10000, "/aComapny/aService/")
      val serializer = new JsonSerializer
      val set = new ServerSet(store, serializer)
      set.join("somehost.domain.com", 1234, immutable.Map("admin" -> 1235))
      set.list must haveKey("somehost.domain.com:1234")
      set.list("somehost.domain.com:1234") must contain("admin" -> 1235)
    }
  }
}
