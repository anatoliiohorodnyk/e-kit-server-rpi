import datetime
import json
import logging
import time

import avahi
import dbus

from utils.sql_database.sql_query import SELECT_OVERALL_POWER_FROM_POWER_CONSUMPTION
from utils.mqtt.mqtt_client import MQTTClient as mqtt
from utils.mqtt.helpers import get_power_supplier_power, get_overall_power


def send_active_devices_topic(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        #        query = f'SELECT PowerConsumption.DeviceId, SUM(PowerConsumption.DeviceConsumption) AS Cons, DevicesInfo.ServiceName FROM PowerConsumption LEFT JOIN DevicesInfo ON PowerConsumption.DeviceId = DevicesInfo.Id ' \
        #                f"GROUP BY DeviceId HAVING Cons > 0"
        query = 'SELECT PowerConsumption.DeviceId, SUM(PowerConsumption.DeviceConsumption) AS Cons, DevicesInfo.ServiceName, updated_services_name.new_name ' \
                'FROM PowerConsumption ' \
                'LEFT JOIN DevicesInfo ' \
                'ON PowerConsumption.DeviceId = DevicesInfo.Id ' \
                'LEFT JOIN updated_services_name ' \
                'ON DevicesInfo.ServiceName = updated_services_name.service_name ' \
                "GROUP BY DeviceId HAVING Cons > 0"
        active_devices = args[0].db_server.execute_query_with_output(query)
        print(f"Active devices: {active_devices}")
        payload = []
        if active_devices:
            for device in active_devices:
                # new_name_query = f'SELECT new_name from updated_services_name WHERE service_name == {device[2]}'
                # service_new_name = args[0].db_server.execute_query_with_output(new_name_query)
                # [{"service_name": <str>, "power": <int>, "id": <int>}]
                # payload = payload + '{' + f'"service_name": "{device[2]}", "power": {device[1]} ,' + f'"name": "{service_new_name[0][0] if service_new_name else device[2]}"' + '},'
                payload.append({"service_name": device[2], "power": device[1], "name": device[3] if device[3] else device[2]})
        subscriber = mqtt()
        subscriber.connect('127.0.0.1', 1883)
        subscriber.publish(topic='active_devices', payload=json.dumps(payload), retain=True)
        time.sleep(1)
        max_power = get_power_supplier_power(args[0].db_server)
        print(f'Max power after removing service: {max_power}')
        current_power = get_overall_power(args[0].db_server)
        print(f'Current power after removing service: {current_power}')
        subscriber.publish(topic='power_consumption_current', payload='{"power": ' + str(max_power) +
                                                                      ', "consumption":' + str(current_power) + '}',
                           retain=True)
        return result

    return wrapper


def send_discovered_devices_topic(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        query = f'SELECT DevicesInfo.Id, DevicesInfo.ServiceName, DevicesInfo.Power, updated_services_name.new_name, CASE WHEN DevicesInfo.IsConsumer THEN ActiveDevices.Cons ELSE 1 END FROM DevicesInfo LEFT JOIN updated_services_name ON updated_services_name.service_name = DevicesInfo.ServiceName ' \
                f' LEFT JOIN (SELECT PowerConsumption.DeviceId AS DeviceId, SUM(PowerConsumption.DeviceConsumption) AS Cons FROM PowerConsumption GROUP BY DeviceId HAVING Cons > 0) ActiveDevices ON ActiveDevices.DeviceId = DevicesInfo.Id'
        active_devices = args[0].db_server.execute_query_with_output(query)
        print(f"Discovered devices: {active_devices}")
        payload = []
        if active_devices:
            for device in active_devices:
                payload.append(
                    {"service_name": device[1], "power": device[2], "name": device[3] if device[3] else device[1],
                     "state": True if device[4] else False})
        subscriber = mqtt()
        subscriber.connect('127.0.0.1', 1883)
        subscriber.publish(topic='discovered_devices', payload=json.dumps(payload), retain=True)
        print(f'Discovered payload: {payload}')
        return result
    return wrapper


class DBusAvahiClient:

    def __init__(self, port=1883, service_name='power_distribution_server', stype='_mqtt._tcp', domain='', host='',
                 text='', db_server=None):
        self.service_name = service_name
        self.port = port
        self.stype = stype
        self.domain = domain
        self.host = host
        self.text = text
        self.db_server = db_server
        self.services_discovered = {}
        self.browser = None
        dbus.mainloop.glib.DBusGMainLoop(set_as_default=True)
        self.bus = dbus.SystemBus()
        self.dbus_avahi_server = dbus.Interface(
            self.bus.get_object(avahi.DBUS_NAME, avahi.DBUS_PATH_SERVER),
            avahi.DBUS_INTERFACE_SERVER)

    def publishService(self):
        group = dbus.Interface(self.bus.get_object(avahi.DBUS_NAME, self.dbus_avahi_server.EntryGroupNew()),
                               avahi.DBUS_INTERFACE_ENTRY_GROUP)

        group.AddService(avahi.IF_UNSPEC, avahi.PROTO_UNSPEC, dbus.UInt32(0),
                         self.service_name, self.stype, self.domain, self.host,
                         self.port, self.text)

        group.Commit()

    def setup_avahi_callbacks(self, stype='_mqtt._tcp', domain=''):
        self.browser = dbus.Interface(
            self.bus.get_object(avahi.DBUS_NAME, self.dbus_avahi_server.ServiceBrowserNew(avahi.IF_UNSPEC,
                                                                                          avahi.PROTO_UNSPEC,
                                                                                          stype,
                                                                                          domain,
                                                                                          dbus.UInt32(0))),
            avahi.DBUS_INTERFACE_SERVICE_BROWSER)
        self.browser.connect_to_signal("ItemNew", self._new_service)

        self.browser.connect_to_signal("ItemRemove", self._remove_service)

    def _new_service(self, interface, protocol, name, stype, domain, flags):
        print(f'Found new service {name}')
        self.services_discovered[name] = DiscoveredAvahiService(
            name=str(name),
            interface=interface,
            protocol=protocol,
            stype=stype,
            domain=domain)
        print(f'Updated: {self.services_discovered}')

    def _if_consumer_active(self, id):
        SQL = f'SELECT State FROM DeviceState WHERE DeviceId == {id} ORDER BY Id DESC LIMIT 1'
        result = self.db_server.execute_query_with_output(SQL)
        if result:
            return result[0][0]
        else:
            return False

    @send_active_devices_topic
    @send_discovered_devices_topic
    def _remove_service(self, interface, protocol, name, stype, domain, flags):
        service_db_data = self.db_server.get_all_from_db(table='DevicesInfo', condition=f"ServiceName == '{name}'")
        if not service_db_data:
            return
        if name in self.services_discovered:
            if self.services_discovered[name].txt:
                print(f'Is consumer: {self.services_discovered[name].txt}')
                if self.services_discovered[name].txt.get('is_consumer'):
                    if self._if_consumer_active(service_db_data[0][0]):
                        overall_power = 0
                        try:
                            overall_power = self.db_server.execute_query_with_output(SELECT_OVERALL_POWER_FROM_POWER_CONSUMPTION)[0][0]
                        except Exception as err:
                            print(err)
                        print(f'Overall power: {overall_power}')
                        self.db_server.insert_in_db('PowerConsumption',
                                                    "DeviceId, DeviceConsumption, OverallConsumption",
                                                    (
                                                        service_db_data[0][0],
                                                        service_db_data[0][3] * -1,
                                                        overall_power - service_db_data[0][3]
                                                    )
                                                    )
                        query = f'SELECT Priority, Blocked FROM DeviceState WHERE DeviceId == {service_db_data[0][0]} ORDER BY Id DESC LIMIT 1'
                        result = self.db.execute_query_with_output(query)[0]
                        self.db.insert_in_db('DeviceState', "DeviceId, Priority, State, IsActive, Blocked",
                                        (
                                            service_db_data[0][0],
                                            result[0], 
                                            False,
                                            False,
                                            result[1]))
                    else:
                        print(
                            f'Device state is not Active, Expected: 1. Actual {self._if_consumer_active(service_db_data[0][0])}')
            del self.services_discovered[name]
            print('Delete service from discovered')
            self.db_server.delete_from_db(
                'DevicesInfo',
                f"ServiceName == '{name}'"
            )
            print(f'Removed from to be power on:{service_db_data[0][0]}')
            self.db_server.delete_from_db(
                'to_be_power_on',
                f"id_device_to_turned_on == '{service_db_data[0][0]}'"
            )


            print('Delete from DB')

    def resolve(self, name):
        service = self.services_discovered.get(name)
        if not service:
            print("Service '%s' has not been discovered yet", name)
            return None

        try:
            reply = self.dbus_avahi_server.ResolveService(service.interface, service.protocol, service.name,
                                                          service.stype,
                                                          service.domain, avahi.PROTO_UNSPEC, dbus.UInt32(0))
            # print("Service '%s' resolved: address %s, port %d", reply[2], reply[7], reply[8])

            # for i in reply:
            #     try:
            #         pass# print(avahi.byte_array_to_string(i))
            #     except Exception:
            #         pass

            # print(reply)
            converted_to_json_dumps = json.dumps(reply[9])
            # print(f'Printing from dumps: {converted_to_json_dumps}')
            converted_to_json_loads = json.loads(converted_to_json_dumps)[0]
            # print(f"Printing from loads: {converted_to_json_loads}")
            string_from_json_loads = ''.join(chr(i) for i in converted_to_json_loads)
            # print(string_from_json_loads)
            service.txt = json.loads(string_from_json_loads)

            # service.address = reply[7]
            # service.port = reply[8]
        except dbus.exceptions.DBusException as e:
            print("Error resolving '%s': %s", name, e)
            service = None

        return service


class DiscoveredAvahiService:
    def __init__(self, name=None, interface=None, protocol=None, stype=None, domain=None, txt=None):
        self.name = name
        self.txt = txt
        self.interface = interface
        self.protocol = protocol
        self.stype = stype
        self.domain = domain
        self.port = 0
        self.address = None
