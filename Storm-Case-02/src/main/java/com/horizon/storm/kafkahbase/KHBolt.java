package com.horizon.storm.kafkahbase;

import org.apache.commons.collections.map.HashedMap;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

import static org.apache.storm.utils.Utils.tuple;

/**
 * Created by admin on 2017/5/22.
 */
public class KHBolt extends BaseRichBolt {
    OutputCollector collector;
    Map<String,Integer> map=new HashMap<String, Integer>();
    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector collector) {
        this.collector = collector;
    }

    @Override
    public void execute(Tuple input) {
        String line=input.getString(0);
        String[] words=line.split(" ");

        for(String word:words){

            if (!word.equals("")){
              //  this.collector.emit(tuple(word,1));
                if(map.containsKey(word)){
                    map.put(word,map.get(word)+1);
                }else {
                    map.put(word,1);
                }
            }
        }

        for (Map.Entry<String,Integer> e:map.entrySet()){
            this.collector.emit(tuple(e.getKey(),e.getValue().toString()));
        }
     this.collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word", "count"));
    }
}
