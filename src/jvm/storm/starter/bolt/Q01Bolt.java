package storm.starter.bolt;

import java.time.LocalDate;
import java.util.Arrays;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/*
SELECT l_returnflag, l_linestatus,
SUM(l_quantity) AS sum_qty,
SUM(l_extendedprice) AS sum_base_price,
SUM(l_extendedprice * (1 - l_discount)) AS sum_disc_price,
SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax)) AS sum_charge,
AVG(l_quantity) AS avg_qty,
AVG(l_extendedprice) AS avg_price,
AVG(l_discount) AS avg_disc,
COUNT(*) AS count_order
FROM lineitem
WHERE l_shipdate <= DATE '1998-12-01' - interval '{0} days'
GROUP BY l_returnflag, l_linestatus
ORDER BY l_returnflag, l_linestatus;
*/



public class Q01Bolt extends BaseRichBolt {
    OutputCollector _collector;
    int	sum_qty=0, count_order=0;
    double sum_base_price=0, sum_disc_price=0, sum_charge = 0, sum_disc=0;
    double avg_qty=0, avg_price=0, avg_disc = 0;
    
    String [] fields = {"l_returnflag", "l_linestatus", "sum_qty", 
    	    "sum_base_price", "sum_disc_price", "sum_charge",
    	    "avg_qty", "avg_price", "avg_disc", "count_order"};
    
    //TODO Subtract {interval} days
    LocalDate date = LocalDate.parse("1998-12-01");

    @Override
    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
      _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {
    	
    	if (date.compareTo(LocalDate.parse(tuple.getString(10))) >= 0) {
    		count_order++;
        	sum_qty 		+= Integer.parseInt(tuple.getString(4));
        	
        	double discount = 1.0 - Double.parseDouble(tuple.getString(6));
        	double tax = 1.0 - Double.parseDouble(tuple.getString(7));
        	double incr_price = Double.parseDouble(tuple.getString(5)); 
        	double incr_discount = incr_price * discount;
        	
        	sum_disc		+= discount;
        	sum_base_price 	+= incr_price;
        	sum_disc_price 	+= incr_discount;
        	sum_charge 		+= incr_discount * tax;
        	
        	avg_qty = sum_qty / (float) count_order;
        	avg_price = sum_base_price / (float) count_order;
        	avg_disc = sum_disc / (float) count_order;
        	
        	_collector.emit(new Values(tuple.getString(8), tuple.getString(9), sum_qty, 
        			sum_base_price,sum_disc_price,sum_charge,avg_qty,avg_price, avg_disc,count_order));
    	    _collector.ack(tuple);
    	}
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declare(new Fields(Arrays.asList(fields)));
    }


  }