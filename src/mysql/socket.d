module mysql.socket;

import std.socket;
import std.exception;

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
		    socket_.shutdown(SocketShutdown.BOTH);
			socket_.close();
			socket_ = null;
		}
	}

	void read(ubyte[] buffer) {
	    for (size_t off, len; off < buffer.length; off += len) {
			len = socket_.receive(buffer[off..$]);
			enforce(len != 0, "Server closed the connection");
			enforce(len != std.socket.Socket.ERROR, "Received std.socket.Socket.ERROR");
		}
	}

	void write(in ubyte[] buffer) {
	    for (size_t off, len; off < buffer.length; off += len) {
        		len = socket_.send(buffer[off..$]);
        		enforce(len != 0, "Server closed the connection");
			enforce(len != std.socket.Socket.ERROR, "Received std.socket.Socket.ERROR");
	    }
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
