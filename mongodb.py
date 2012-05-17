#
# Plugin to collectd statistics from MongoDB
#

import collectd
from pymongo import Connection


class MongoDB(object):

    def __init__(self):
        self.plugin_name = "mongodb"
        self.mongo_hosts = []
        
        #self.mongo_db = ["admin", ]
        #self.mongo_user = None
        #self.mongo_password = None

        #self.lockTotalTime = None
        #self.lockTime = None

    def submit(self, type, instance, value, db=None, host=None, port=27017):
        if db:
            plugin_instance = '%s-%s' % (port, db)
        else:
            plugin_instance = str(port)
        v = collectd.Values()
        if host:
            v.host = host
        v.plugin = self.plugin_name
        v.plugin_instance = plugin_instance
        v.type = type
        v.type_instance = instance
        v.values = [value, ]
        #collectd.warning("dispatching values: %s, %s, %s, %s, %d," %(host,type,instance,plugin_instance,value))
        v.dispatch()

    def do_server_status(self):
        for mongo in self.mongo_hosts:
            self.do_host_status(mongo)

    def do_host_status(self,mongo):
        con = Connection(host=mongo['host'], port=mongo['port'], slave_okay=True)
        db = con['admin']
        if mongo['user'] and mongo['password']:
            db.authenticate(mongo['user'], mongo['password'])

        server_status = db.command('serverStatus')

        # operations
        for k, v in server_status['opcounters'].items():
            self.submit('mongo_total_operations', k, v, host=mongo['host'], port=mongo['port'])

        # memory
        for t in ['resident', 'virtual', 'mapped']:
            self.submit('mongo_memory', t, server_status['mem'][t], host=mongo['host'], port=mongo['port'])

        # connections
        self.submit('mongo_connections', 'connections', server_status['connections']['current'],host=mongo['host'], port=mongo['port'])

        # locks
        if mongo['lockTotalTime'] is not None and mongo['lockTime'] is not None:
            #collectd.warning( "total %d - %d / %d - %d " % (server_status['globalLock']['totalTime'], self.lockTotalTime, server_status['globalLock']['totalTime'], self.lockTime))
            if mongo['lockTime']==server_status['globalLock']['lockTime']:
                value=0.0
            else:
                value=float (server_status['globalLock']['lockTime'] - mongo['lockTime']) *100.0 / float(server_status['globalLock']['totalTime'] - mongo['lockTotalTime'] ) 
            #collectd.warning( "Submitting value %d " % value)
            self.submit('mongo_percent', 'lock_ratio', value, host=mongo['host'], port=mongo['port'])


        mongo['lockTotalTime']=server_status['globalLock']['totalTime']
        mongo['lockTime']=server_status['globalLock']['lockTime']


        # indexes
        accesses = server_status['indexCounters']['btree']['accesses']
        misses = server_status['indexCounters']['btree']['misses']
        if misses:
            self.submit('mongo_cache_ratio', 'cache_misses', accesses / float(misses),host=mongo['host'], port=mongo['port'])
        else:
            self.submit('mongo_cache_ratio', 'cache_misses', 0, host=mongo['host'], port=mongo['port'])

        for mongo_db in mongo['db']:
            db = con[mongo_db]
            if mongo['user'] and mongo['password']:
                db.authenticate(mongo['user'], mongo['password'])
            db_stats = db.command('dbstats')

            # stats counts
            self.submit('mongo_counter', 'object_count', db_stats['objects'], mongo_db, host=mongo['host'], port=mongo['port'])
            self.submit('mongo_counter', 'collections', db_stats['collections'], mongo_db, host=mongo['host'], port=mongo['port'])
            self.submit('mongo_counter', 'num_extents', db_stats['numExtents'], mongo_db, host=mongo['host'], port=mongo['port'])
            self.submit('mongo_counter', 'indexes', db_stats['indexes'], mongo_db, host=mongo['host'], port=mongo['port'])

            # stats sizes
            self.submit('mongo_file_size', 'storage', db_stats['storageSize'], mongo_db, host=mongo['host'], port=mongo['port'])
            self.submit('mongo_file_size', 'index', db_stats['indexSize'], mongo_db, host=mongo['host'], port=mongo['port'])
            self.submit('mongo_file_size', 'data', db_stats['dataSize'], mongo_db, host=mongo['host'], port=mongo['port'])

        con.disconnect()

    def config(self, obj):
        #collectd.warning("Mongo config started")
        for node in obj.children:
          if node.key.lower() == "host":
            port=27017
	    if len(node.values)>=1:
		hostname=node.values[0]
		if len(node.values)>=2:
		    port=int(node.values[1])
	    else:
		hostname="localhost"
            dbs      = list()
            user     = None
            password = None
            for hostchild in node.children:
                if hostchild.key.lower() == "db":
                    if len(hostchild.values)>=1:
                        dbs.append( hostchild.values[0] )
                    else:
                        collectd.warning("mongodb plugin: configuration error")
                if hostchild.key.lower() == "user":
                    if len(hostchild.values)>=1:
                        user = hostchild.values[0]
                    else:
                        collectd.warning("mongodb plugin: configuration error")
                if hostchild.key.lower() == "password":
                    if len(hostchild.values)>=1:
                        password = hostchild.values[0]
                    else:
                        collectd.warning("mongodb plugin: configuration error")
            self.mongo_hosts.append({'host':hostname,
                                     'port':port,
                                     'db':dbs,
                                     'user':user,
                                     'password':password,
                                     'lockTotalTime':None,
                                     'lockTime':None,
                                     })
            #collectd.warning("Mongo config, Added host: %s, %s, %s" %(hostname,port,dbs))

mongodb = MongoDB()
collectd.register_read(mongodb.do_server_status)
collectd.register_config(mongodb.config)
