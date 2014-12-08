package storm.starter.bolt;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class SelectBolt extends BaseRichBolt {
    OutputCollector _collector;
    String field;
    String label;
    int		col;
    
    public SelectBolt (List<String> schema, String _field, String _label) {
    	this.field = _field;
    	this.label = _label;
    	this.col = schema.indexOf(_field);
    }

    public SelectBolt (List<String> schema, String _field) {
    	this(schema, _field, _field);
    }

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
	    _collector.emit(new Values(tuple.getString(col)));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	String [] fields = {field};
      declarer.declare(new Fields(Arrays.asList(fields)));
    }


  }


