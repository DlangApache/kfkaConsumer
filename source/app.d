import std.stdio;
import zookeeperhandle;
import consumer;
import core.thread;
import std.file;
import std.conv;

void main()
{
	string topic = "test";
	string groupId = "group.id";
	string value = "foo";
	string commitTime = "100";
	int partition = 0;
	writeln("thread id = ", Thread.getThis.id);
	auto zook = new ZookeeperHandle( "localhost:2181");
	if(!zook.zhandle){
		writeln("zookeeper init erro");
		return;
	}
	auto kafka = new Consumer(zook);
	if(!kafka.rdKafaka){
		writeln("kafaka init erro");
		return;
	}
	auto tpic = kafka.createTopic(topic,groupId,value,commitTime);
	bool first = true;
	bool isExistsFile = true;
	
	string fileName = topic ~ "-" ~ to!string(partition) ~ ".offset";
	if (exists(fileName))
	{
		writeln(fileName, "===============exists");
	}
	else
	{
		isExistsFile = false;
		File file = File(fileName, "w");
		file.writeln(0);
		file.close();
	}

	void callFun(rd_kafka_message_t *message)
	{
		if (first && isExistsFile) {
			first = false;
			writeln("no first=========");
			return;
		}
		writeln("\ncall back fun thread id = ", Thread.getThis.id);
		if (message.err == rd_kafka_resp_err_t.RD_KAFKA_RESP_ERR__PARTITION_EOF)
		{
			writeln(topic ,"is rof");
			return;
		}

		writeln("get key : ", cast(string)(message.key[0..message.key_len]),"  length: ", message.len);
		writeln("get Data: ", cast(string)(message.payload[0 .. message.len]), "  length: ", message.len);
	}
	tpic.startOffset(partition, RD_KAFKA_OFFSET_STORED);
	writeln("\n\n-------------------------------------------\n");
	
	bool run = true;
	while (run) {
		
		writeln("\nrun return : ",tpic.feach(partition, &callFun,100000), "\n");
	}

	writeln("Edit source/app.d to start your project.");
}
