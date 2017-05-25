package com.horizon.storm.kafkahbase;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.hbase.bolt.HBaseBolt;
import org.apache.storm.hbase.bolt.mapper.SimpleHBaseMapper;
import org.apache.storm.kafka.*;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by admin on 2017/5/22.
 */
public class KHTopology {
    //Core Spout

//    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String SPOUTID="kafkaSpout";//当前spout的唯一标识
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String HBASE_BOLT = "HBASE_BOLT";
    private static final String TOPOLOGY_ID = "KHTopology";



    public static void main(String[] args){

         //从zookeeper动态读取broker
        BrokerHosts hosts = new ZkHosts("172.17.11.120:2181,172.17.11.117:2181,172.17.11.118:2181");
        String topic="TOPIC-STORM-HBASE";
        String zkRoot="/storm";//用于存储当前处理到哪个Offset
        SpoutConfig spoutConfig = new SpoutConfig(hosts, topic, zkRoot, SPOUTID);
        //spoutConfig.forceFromStart = true;
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());//如何解码数据

        TopologyBuilder builder=new TopologyBuilder();

        KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
         //HBase

        Config config = new Config();
        config.setDebug(true);
        Map<String, Object> hbConf = new HashMap<String, Object>();


        hbConf.put("hbase.rootdir","hdfs://master:9000/hbase");
        hbConf.put("hbase.zookeeper.quorum","master,slave1,slave2");
        config.put("hbase.conf", hbConf);


        SimpleHBaseMapper mapper = new SimpleHBaseMapper()
                .withRowKeyField("word")
                .withColumnFields(new Fields("count"))
                .withColumnFamily("result");

        HBaseBolt hbase = new HBaseBolt("Wordcount", mapper)
                .withConfigKey("hbase.conf");

        KHBolt bolt=new KHBolt();

        builder.setSpout(SPOUTID,new KafkaSpout(spoutConfig),1);//在Topology中加入Spout的代码
        builder.setBolt(COUNT_BOLT,bolt,1).shuffleGrouping(SPOUTID);
        builder.setBolt(HBASE_BOLT,hbase,1).fieldsGrouping(COUNT_BOLT,new Fields("word"));

        if (args != null && args.length > 0) {
            try {
                StormSubmitter.submitTopology(TOPOLOGY_ID, config,
                        builder.createTopology());
            } catch (Exception e) {
                e.printStackTrace();
            }
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(TOPOLOGY_ID, config,
                    builder.createTopology());
        }

    }
}
