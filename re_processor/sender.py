from re_processor.mixins.transceiver import BaseRabbitmqConsumer


class MainSender(BaseRabbitmqConsumer):

    def __init__(self, product_key=None):
        self.product_key = product_key or '*'
        self.mq_initial()

    def send(self, msg, product_key=None):
        self.mq_publish(product_key or self.product_key, [msg])

    def send_msgs(self, msgs):
        self.mq_publish(self.product_key, msgs)
