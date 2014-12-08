/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package storm.starter.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;
import java.util.Random;

public class FileReaderSpout extends BaseRichSpout {
  SpoutOutputCollector _collector;
  Random _rand;
  BufferedReader source = null;


  @Override
  public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
    _collector = collector;
    _rand = new Random();
    String file = "input.txt";
    
    try {
		source = new BufferedReader(new FileReader(file));
	} catch (FileNotFoundException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
  }

  @Override
  public void nextTuple() {
    Utils.sleep(100);
    String line = null;
    try {
    	line = source.readLine();
//    	if (line == null) {
//    		source.reset();
//    		line = source.readLine();
//    	}
    }
    catch (Exception e) {
    	e.printStackTrace();
    }
//    String[] sentences = new String[]{ "RED ORANGE YELLOW GREEN BLUE", "AAAA BBB CCCC DDD EEE FFF",
//        "THE QUICK BROWN FOX JUMPS", "HELLO MY NAME IS JOHN", "i am at two with nature" };
//    String sentence = sentences[_rand.nextInt(sentences.length)];
    _collector.emit(new Values(line));
  }

  @Override
  public void ack(Object id) {
  }

  @Override
  public void fail(Object id) {
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields("word"));
  }

}