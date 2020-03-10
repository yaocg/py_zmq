from zmq_builder import AbsZmqSubscribe
import time

class subscribe(AbsZmqSubscribe):
    def __init__(self, zmq_str, zmq_msg_type='', zmq_timeout_ms=2500):
        AbsZmqSubscribe.__init__(self, zmq_str, zmq_msg_type, zmq_timeout_ms)

    def message_dispatcher(self, msg_list):
        if not msg_list:
            print("zmq msg_type:{0} get_empty_msg:{1}".format(self.get_msg_type(), msg_list))
            return

        msg_type_bytes = msg_list[0]
        msg_type = msg_type_bytes.decode('utf-8')
        if msg_type != self.get_msg_type():
            print("zmq msg_type:{0} get_invalid_msg:{1}".format(self.get_msg_type(), msg_list))
            return

        print("zmq msg_type:{0} recv_one_msg:{1}".format(self.get_msg_type(), msg_list))


if __name__ == "__main__":
    sub = subscribe("tcp://127.0.0.1:8331", "hashtx", 1200*1000)
    sub.start()

    while 1:
        time.sleep(1)

