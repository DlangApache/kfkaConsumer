module zookeeperhandle;

public import deimos.zookeeper.zookeeper;

public import std.experimental.logger;
import std.string;
import std.json;

import std.stdio;

alias WatchZookeeper = void delegate(ZookeeperHandle handle, int type, int state, string path);

class ZookeeperHandle
{
	this(string zookeeper,int timeout = 10000)
	{
		debug{ zoo_set_debug_level(ZooLogLevel.ZOO_LOG_LEVEL_DEBUG);}
		_zh = zookeeper_init(toStringz(zookeeper), &watcher, timeout, null, cast(void *)this, 0);
	}

	JSONValue[] getChriedInfo(string _path)
	{
		if(!_zh) return null;
		String_vector brokerlist;
		trace(" path == ", _path);
		if (zoo_get_children(_zh,toStringz(_path), 1, &brokerlist) != ZOO_ERRORS.ZOK) return null;
		scope(exit)deallocate_String_vector(&brokerlist);
		int len;
		char[2048] cfg;
		JSONValue[] rvalue;
		for (int i = 0; i < brokerlist.count; i++){
			len = 2048;
			//trace("brokerlist data = ");
			string path = _path ~ "/" ~ cast(string)(fromStringz(brokerlist.data[i]));
			trace("get path == ", path);
			zoo_get(_zh, toStringz(path), 0, cfg.ptr, &len, null);
			try{
				if(len <= 0) continue;
				auto data = cfg[0..len];
				trace(" data  = ", data);
				rvalue ~= parseJSON(data);
			} catch {
				error("data to json erro!");
				continue;
			}
		}
		return rvalue;
	}

	@property watch(WatchZookeeper wh){_watch = wh;}

	~this(){
		if(_zh){
			zookeeper_close(_zh);
		}
	}

	@property zhandle(){return _zh;}
private:
	zhandle_t * _zh;
	WatchZookeeper _watch;
}

private:
extern (C):
void watcher(zhandle_t *zh, int type, int state, const char *path, void *watcherCtx)
{
	trace("zookeeprt handle callback, type = ", type, "  path = ", fromStringz(path));
	ZookeeperHandle handle = cast(ZookeeperHandle)watcherCtx;
	if(handle._watch){
		handle._watch(handle,type,state,cast(string)(fromStringz(path)));
	}
}
