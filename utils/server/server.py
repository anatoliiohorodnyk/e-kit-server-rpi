from utils.dbus_client.dbus_avahi_client import DBusAvahiClient
from utils.mqtt.mqtt_client import MQTTClient as mqtt
from utils.sql_database.database_server import ServerDataBase


class Server:
    def __init__(self):
        subscriber = mqtt()
        subscriber.connect('127.0.0.1', 1883)
        self.db_server = ServerDataBase()
        self.avahi_server = DBusAvahiClient(db_server=self.db_server)
        self.avahi_server.setup_avahi_callbacks()
        #self.check_if_server_is_present()
        self.avahi_server.publishService()
        subscriber.db_server = self.db_server
        subscriber.avahi_server = self.avahi_server
        subscriber.loop_start()
        subscriber.subscribe("+/set.to_server")
        subscriber.subscribe("+/get.to_server")

    def wait_for_update(self):
        pass

    def check_if_server_is_present(self):
        if self.avahi_server.services_discovered['power_distribution_server']:
            exit(0)
