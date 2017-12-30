package io.github.omalperera

import java.text.SimpleDateFormat
import java.util.{Calendar, Properties, UUID}

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.util.Random
import scala.util.control.Breaks

object dataGenerator {
  def main(args: Array[String]): Unit = {

    //default values
    val default_kafkaNode = "localhost:9092"
    val default_kafkaTopics = "test"
    val default_events = "0"
    val default_recordInterval = "5" //in Seconds
    val default_randomStart = "0" //in Seconds
    val default_randomEnd = "500" //in Seconds


    //Assigning values if command line arguments are empty
    val brokers = util.Try(args(0)).getOrElse(default_kafkaNode)
    val topic = util.Try(args(1)).getOrElse(default_kafkaTopics)
    val intervalEvent = util.Try(args(3)).getOrElse(default_recordInterval).toInt


    val events = default_events.toInt
    val randomStart = default_randomStart.toInt
    val randomEnd = default_randomEnd.toInt
    val clientId = UUID.randomUUID().toString()


    //Kafka properties
    val properties = new Properties()
    properties.put("bootstrap.servers", brokers)
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("client.id", clientId)

    val producer = new KafkaProducer[String, String](properties)

    println("*********** RDS Simulator Started to feed kafka topic '" + topic + "' ***********")

    val citiesInNewYork = List("Elmira", "Oneonta", "Peekskill", "Albany", "Yonkers")
    val rnd = new Random()
    val rnd2 = new Random()

    var i = 0

    val loop = new Breaks()

    //The while loop will generate the data and send to Kafka
    loop.breakable{
      while(true){

        val n = randomStart + rnd2.nextInt(randomEnd - randomStart + 1)
        for(i <- Range(0, n)){
          val today = Calendar.getInstance.getTime
          val formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          val key = UUID.randomUUID().toString().split("-")(0)
          val value = formatter.format(today) + "," + citiesInNewYork(rnd.nextInt(citiesInNewYork.length))

          val data = new ProducerRecord[String, String](topic, key, value)

          //println("--- topic: " + topic + " ---")
          //println("key: " + data.key())
          //println("value: " + data.value() + "\n")
          println(data.value())
          producer.send(data)
        }

        val k = i + 1
        println(s"--- #$k: $n records in [$randomStart, $randomEnd] ---")

        if(intervalEvent > 0)
          Thread.sleep(intervalEvent * 1000)

        i += 1
        if(events > 0 && i == events)
          loop.break()
      }
    }
  }

  }
