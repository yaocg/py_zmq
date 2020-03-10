import zmq
import threading
from abc import ABCMeta,abstractmethod

class AbsZmqSubscribe(threading.Thread, metaclass=ABCMeta):
    """zmq 订阅客户端 - 超时重连
    """
    def __init__(self, conn_str="tcp://127.0.0.1:8331", msg_type='', timeout_ms=2500):
        threading.Thread.__init__(self)

        self.__conn_str = conn_str
        self.__msg_type = msg_type
        self.__timeout_ms = timeout_ms

        self.__context = zmq.Context(1)
        self.__poll = zmq.Poller()

        self.__connect()


    def get_msg_type(self):
        return self.__msg_type

    def __del__(self):
        self.__close()

    def __connect(self):
        # Create new connection
        self.__socket = self.__context.socket(zmq.SUB)
        self.__socket.connect(self.__conn_str)
        self.__socket.setsockopt(zmq.SUBSCRIBE, self.__msg_type.encode('utf-8'))
        self.__poll.register(self.__socket, zmq.POLLIN)

    def __close(self):
        # Socket is confused. Close and remove it.
        self.__socket.setsockopt(zmq.LINGER, 0)
        self.__socket.close()
        self.__poll.unregister(self.__socket)

    @abstractmethod
    def message_dispatcher(self, msg_list):
        pass

    def run(self):
        while True:
            socks = dict(self.__poll.poll(self.__timeout_ms))
            if socks.get(self.__socket) == zmq.POLLIN:
                msg_list = self.__socket.recv_multipart()
                self.message_dispatcher(msg_list)
            else:
                print("zmq timeout. {0}".format(self.__conn_str))
                self.__close()
                print("zmq Reconnecting. {0}".format(self.__conn_str))
                self.__connect()
