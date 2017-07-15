module mysql.socket;

import std.socket;

struct Socket {
	void connect(const(char)[] host, ushort port) {
		socket_ = new TcpSocket();
		socket_.connect(new InternetAddress(host, port));
		socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.KEEPALIVE, true);
		socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.TCP_NODELAY, true);
	}

	bool connected() inout {
		return socket_ && socket_.isAlive();
	}

	void close() {
		if (socket_) {
			socket_.close();
			socket_ = null;
		}
	}

	void read(ubyte[] buffer) {
		socket_.receive(buffer);
	}

	void write(in ubyte[] buffer) {
		socket_.send(buffer);
	}

//	void flush() {
//		socket_.flush();
//	}
//
//	bool empty() {
//		return socket_.empty;
//	}
private:
	TcpSocket socket_;
}