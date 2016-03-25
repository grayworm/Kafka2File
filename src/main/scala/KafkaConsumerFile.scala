import java.io.File
import java.util.Properties
import java.util.Date
import kafka.consumer._
import java.text.SimpleDateFormat
import sys.process._


object KafkaConsumerFile {
  def main(args: Array[String]) {
    /**
     * 参数含义
     * java -jar streams8:2181 park_group evt_imei /ouidir/ BI_LOST 1 101
     */
    val Array( zkQuorum,groupId, topic, outputdir,file_pre,colum,constant) = args
    println("start_time:"+new Date)
    val topicMap = topic.split(",").map((_,1)).toMap
    val props = new Properties()
    props.put("zookeeper.connect", zkQuorum)
    props.put("group.id",groupId)
    props.put("zookeeper.session.timeout.ms", "400")
    props.put("zookeeper.sync.time.ms", "200")
    props.put("auto.commit.interval.ms", "1000")
    val config = new ConsumerConfig(props)
    val connector = Consumer.create(config)
    val stream = connector.createMessageStreams(topicMap).get(topic).get(0)
    for(messageAndTopic <- stream) {
      val message = new String(messageAndTopic.message).split(",")
      var line = ""
        if(colum.toInt == 1) {
          val colum_0 = message(0)
          line = colum_0
        }
        if(colum.toInt == 2) {
          val colum_0 = message(0)
          val colum_1 = message(1)
          line = colum_0+"|"+colum_1
        }
        if(colum.toInt == 3) {
          val colum_0 = message(0)
          val colum_1 = message(1)
          val colum_2 = message(2)
          line = colum_0+"|"+colum_1+"|"+colum_2
        }
        if(colum.toInt == 4) {
          val colum_0 = message(0)
          val colum_1 = message(1)
          val colum_2 = message(2)
          val colum_3 = message(3)
          line = colum_0+"|"+colum_1+"|"+colum_2+"|"+colum_3
        }
        if(colum.toInt == 5) {
          val colum_0 = message(0)
          val colum_1 = message(1)
          val colum_2 = message(2)
          val colum_3 = message(3)
          val colum_4 = message(4)
          line = colum_0+"|"+colum_1+"|"+colum_2+"|"+colum_3+"|"+colum_4
        }
        if(constant!=""){
          line=line+"|"+constant
        }
        val time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
        val fileName = outputdir + File.separator + file_pre + "_" + time + "_01.tmp"
        val cmd = "echo " + line
          cmd #>> new File(fileName) !
      }
//        val time = new SimpleDateFormat("yyyyMMddHHmmss").format(new Date())
//        println(time)
      }
  }
