module mysql.pool;

import core.thread;
import std.datetime;

import mysql;

final class ConnectionPool
{
	private
	{
		class PooledConnection
		{
			bool isBusy;
			Connection connection;
			
			this(Connection connection)
			{
				this.connection = connection;
			}
		}
		
		static bool _instantiated;
		__gshared ConnectionPool _instance;
		
		string _host;
		string _user;
		string _password;
		string _database;
		ushort _port;
		ConnectionOptions _options;
		CapabilityFlags _caps;
		
		int _maxConnections = 50; 			// 连接池最大的大小
		int _initialConnections = 10;		// 初始连接数
		int _incrementalConnections = 5;	// 每次创建的增量
		
		PooledConnection[] _connections;	// 连接池
	}
	
	private this(string host, string user, string password, string database, ushort port = 3306,
		uint maxConnections = 50, uint initialConnections = 10, uint incrementalConnections = 5,
		ConnectionOptions options = ConnectionOptions.Default, CapabilityFlags caps = DefaultClientCaps)
	{
		this._host = host;
		this._user = user;
		this._password = password;
		this._database = database;
		this._port = port;
		this._maxConnections = maxConnections;
		this._initialConnections = initialConnections;
		this._incrementalConnections = incrementalConnections;
		this._options = options;
		this._caps = caps;
		
		createConnections(initialConnections);
	}
	
	static ConnectionPool getInstance(string host, string user, string password, string database, ushort port = 3306,
		uint maxConnections = 50, uint initialConnections = 10, uint incrementalConnections = 5,
		ConnectionOptions options = ConnectionOptions.Default, CapabilityFlags caps = DefaultClientCaps)
    {
        if (!_instantiated)
        {
            synchronized(ConnectionPool.classinfo)
            {
                if (!_instance)
                {
                    _instance = new ConnectionPool(
						host,
						user,
						password,
						database,
						port,
						maxConnections,
						initialConnections,
						incrementalConnections,
						options,
						caps
					);
                }
  
                _instantiated = true;
            }
        }
  
        return _instance;
    }
    
	private void createConnections(uint num)
	{
		for (int i; i < num; i++)
		{
			// _maxConnections 设置为负数表示不限连接数
			if ((_maxConnections > 0) && (_connections.length >= _maxConnections))
			{
				break;
			}
			
			Connection conn = new Connection(
					this._host,
					this._user,
					this._password,
					this._database,
					this._port,
					this._options,
					this._caps);
			_connections ~= new PooledConnection(conn);
					
		}
	}

	Connection getConnection()
	{
		synchronized(ConnectionPool.classinfo)
		{
			Connection conn = getFreeConnection();
			
			if (conn is null)
			{
				Thread.sleep(250.msecs);
				conn = getFreeConnection();
			}
	    
		    return conn;
		}
	}

	private Connection getFreeConnection()
	{
		Connection conn = findFreeConnection();
		
        if (conn is null)
		{
			createConnections(_incrementalConnections);
			conn = findFreeConnection();
        }     

        return conn;
	}

    private Connection findFreeConnection()
	{
        Connection conn = null;
        
        foreach(pooled; _connections)
        {
        	if (!pooled.isBusy)
        	{
        		conn = pooled.connection;
        		pooled.isBusy = true;
        		
        		if (!testConnection(conn))
        		{
        			conn = new Connection(
						this._host,
						this._user,
						this._password,
						this._database,
						this._port,
						this._options,
						this._caps);
        		}
        		
        		break;
        	}
        }
        
        return conn;
	}

	private bool testConnection(Connection conn)
	{
		try
		{
			conn.ping();
			return true;
		}
		catch (Exception e)
		{
			return false;
		}
    }
	
    void releaseConnection(Connection conn)
	{
		foreach(pooled; _connections)
		{
			if (pooled.connection == conn)
			{
				pooled.isBusy = false;
				
				break;
			}
		}
	}
}