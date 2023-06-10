package PracticeKafka

import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}

import java.util.{Collections, Properties}
import scala.jdk.CollectionConverters._

object TopicsEx {
  def main(args: Array[String]): Unit = {
    val bootstrapServer = "localhost:9092"
    val numPartitions = 1
    val topicName = "json-data"

    //Configuring AdminClient
    val props = new Properties()
    props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer)

    //creating topic
    val adminClient = AdminClient.create(props)

    val newTopic = new NewTopic(topicName,numPartitions,1.toShort)
    adminClient.createTopics(List(newTopic).asJavaCollection)

    //Describing Topic
    val descTopic = adminClient.describeTopics(Collections.singleton(topicName)).values().get(topicName).get()
    println("Topic Description:")
    println(descTopic)

    //Partitions
    val partitions = descTopic.partitions().asScala

    partitions.foreach(records => println(
      s"Partitions ${records.partition()} leader: ${records.leader()} Replicas :"
    ))



    //Listing Topics
    val listTopic = adminClient.listTopics().names().get()
    println(s"Name of the Topics are : $listTopic")
/*
    //Delete Topics
    val deleteTopics = adminClient.deleteTopics(Collections.singleton("my-topic")).all().get()
    println(s"Topic deleted ${listTopic}")


 */

  }

}
