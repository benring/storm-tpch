package storm.starter.bolt;
import java.util.List;

import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class CountAggregator  extends SelectBolt {

    int 	count = 0;

    public CountAggregator (List<String> schema, String asLabel) {
    	super(schema, "count", asLabel);
    }

    public CountAggregator (List<String> schema) {
    	this(schema, "count");
    }

    @Override
    public void execute(Tuple tuple) {
    	count++;
	    _collector.emit(new Values(label, count));
	    _collector.ack(tuple);
    }

}