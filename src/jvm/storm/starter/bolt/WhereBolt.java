package storm.starter.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import storm.starter.sql.SQLComparer;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class WhereBolt extends BaseRichBolt {
    OutputCollector _collector;
    String []	fields;
    int			col;
    SQLComparer cmp;
    
    
    public WhereBolt (String [] _fields, String sel, SQLComparer _cmp) {
    	this.fields = _fields;
    	this.col = Arrays.asList(fields).indexOf(sel);
    	this.cmp = _cmp;
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	String elm = tuple.getString(col);
    	
    	if (cmp.Compare(cmp.Parse(elm)))  {
    	      _collector.emit(tuple.getValues());
    	      _collector.ack(tuple);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
      declarer.declare(new Fields(Arrays.asList(fields)));
    }


  }

