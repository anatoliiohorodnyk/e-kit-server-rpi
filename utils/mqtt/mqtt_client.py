import json

import paho.mqtt.client as mqtt

from utils.mqtt.helpers import set_commands_handler, get_commands_handler, get_overall_power, get_power_supplier_power


class MQTTClient(mqtt.Client):
    def __init__(self, db_server=None, avahi_server=None):
        super().__init__()
        self.db_server = db_server
        self.avahi_server = avahi_server



#    @staticmethod
#    def on_subscribe(client, userdata, mid, granted_qos):
#        max_power = get_overall_power(client.db_server)
#        current_power = get_power_supplier_power(client.db_server)
#        client.publish(topic='power_consumption_current', payload='{"max_power": ' + str(max_power) +
#                                                                  ', "current_power":' + str(current_power) + '}'
#                       )

#    @staticmethod
#    def on_unsubscribe(client, userdata, mid):
#        max_power = get_overall_power(client.db_server)
#        current_power = get_power_supplier_power(client.db_server)
#        client.publish(topic='power_consumption_current', payload='{"max_power": ' + str(max_power) +
#                                                                  ', "current_power":' + str(current_power) + '}')

    @staticmethod
    def on_message(client, userdata, msg):
        """Store all received messages under 'received' attribute as topic: payload pairs"""
        if msg.retain == 1 and client.skip_retained:
            print("MQTT. Skip retained msg: " + msg.topic + " " + str(msg.qos) + " " + str(msg.payload))
        else:
            raw = msg.payload.decode("UTF-8")
            try:
                client.last = json.loads(raw)
            except ValueError:
                client.last = raw
            print("MQTT received: topic: " + msg.topic + " qos: " + str(msg.qos) + " message: " + str(client.last))
            if 'set.to_server' in msg.topic:
                print(msg)
                set_commands_handler(client, msg, client.avahi_server, client.db_server)
            elif 'get.to_server' in msg.topic:
                print(msg)
                get_commands_handler(client, msg, client.avahi_server, client.db_server)