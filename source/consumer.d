module consumer;

public import deimos.librdkafka;
import zookeeperhandle;
import std.json;
import std.string;
import std.typetuple;
import std.conv;
import std.typecons;

import std.stdio;

class Consumer
{
	this(ZookeeperHandle zooker,string zookerpath = "/brokers/ids")
	{
		this._zooker = zooker;
		_path = zookerpath;
		auto json = _zooker.getChriedInfo(zookerpath);
		trace("zookpeeer chind = ", json);
		if(json.length == 0) return;
		string hosts;
		foreach(ref jvalue;json){
			try{
				auto host = jvalue["host"].str;
				auto port = jvalue["port"].integer;
				hosts ~= host;
				hosts ~= ":";
				hosts ~= to!string(port);
				hosts ~=  ",";
			}catch{continue;}
		}
		if(hosts.length == 0) return;
		trace("hosts chind = ", hosts);
		conf = rd_kafka_conf_new();

		char[512] erro;
		if(rd_kafka_conf_set(conf,"metadata.broker.list",toStringz(hosts),erro.ptr,512) != rd_kafka_conf_res_t.RD_KAFKA_CONF_OK) return;
		rk = rd_kafka_new(rd_kafka_type_t.RD_KAFKA_CONSUMER, conf, erro.ptr,512);
		zooker.watch = &watchZook;
	//	rd_kafka_set_log_level(rk, LOG_DEBUG);
	}

	ConsumerTopic createTopic( string topicname,string groupId = "", string value = "",string commitTime = "10000", int startOffest = RD_KAFKA_OFFSET_STORED)
	{
		trace("createTopic : topicname = ", topicname, "  groupId = ",groupId," value = ",value, " commitTime = ",commitTime);
		auto topic = new ConsumerTopic;
		char[512] erro;
		topic.topic_conf = rd_kafka_topic_conf_new();
		rd_kafka_topic_conf_set(topic.topic_conf, toStringz(groupId), toStringz(value), erro.ptr,512);
		rd_kafka_topic_conf_set(topic.topic_conf,toStringz("auto.commit.interval.ms"), toStringz(commitTime), erro.ptr,512);
		topic.topic =  rd_kafka_topic_new(rk, toStringz(topicname), topic.topic_conf);
		topic._groupId = groupId;
		topic._value = value;
		topic._topic = topicname;
		topic._commitTime = commitTime;
		return topic;
	}

	~this(){
		if(rk){
			rd_kafka_destroy(rk);
		} else {
			if(conf) rd_kafka_conf_destroy(conf);
		}//释放rd时，其关联的config也会被释放
	}

	@property zookeeper() {return _zooker;}
//	@property string groupID() {return _groupId;}
	@property rdKafaka(){return rk;}
//	@property rdKafakaConfig(){return conf;}
protected:
	void watchZook(ZookeeperHandle handle, int type, int state, string path)
	{
		trace("watchZook , type = ", type, "  path = ", path, " this _path = ", _path);
		if (type == ZOO_CHILD_EVENT && path == _path){
			auto json = _zooker.getChriedInfo(_path);
			trace("zookpeeer chind = ", json);
			string hosts;
			foreach(ref jvalue;json){
				try{
					auto host = jvalue["host"].str;
					auto port = jvalue["port"].integer;
					hosts ~= host;
					hosts ~= ":";
					hosts ~= to!string(port);
					hosts ~=  ",";
				}catch{continue;}
			}
			rd_kafka_brokers_add(rk, toStringz(hosts));
			rd_kafka_poll(rk, 10);
		}
	}
private:
	ZookeeperHandle _zooker;
	rd_kafka_t *rk = null;
	rd_kafka_conf_t *conf = null;
	string _path;

}


class ConsumerTopic
{

	int feach(int partition,void delegate(rd_kafka_message_t *) message, int timout = 10000)
	{
		writeln("\n\n begin feach!");
		if(!topic) return -1;
		if(!message) return -1;
		callBack back;
		back.fun = message;
		return rd_kafka_consume_callback(topic,partition,timout,&consume_call_back,&back); 
	}

	void startOffset(int partition,int startOffset)
	{
		rd_kafka_consume_start(topic, partition, startOffset);
	}

	~this()
	{
		if(topic){
			rd_kafka_topic_destroy(topic);
		} else if(topic_conf){
			rd_kafka_topic_conf_destroy(topic_conf);
		}
	}
private:
	this(){}
	rd_kafka_topic_conf_t *topic_conf = null;
	rd_kafka_topic_t * topic = null;
	string _groupId ;
	string _value;
	string _commitTime;
	string _topic;
}
private:

struct callBack{
	void delegate(rd_kafka_message_t *) fun;
}

extern (C):
void consume_call_back(rd_kafka_message_t*rkmessage,void *opaque)
{
	auto del = cast(callBack *)opaque;
	try{
		del.fun(rkmessage);
	} catch {}
}
