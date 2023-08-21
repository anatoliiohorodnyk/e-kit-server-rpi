
from utils.sql_database.sql_query import SELECT_POWER_SUPPLIER_FROM_DEVICE_INFO, \
    SELECT_OVERALL_POWER_FROM_POWER_CONSUMPTION

import json
import logging
import time
from sqlalchemy import text 

on_request_result = []

def get_power_supplier_power(db):
    power = db.execute_query_with_output(SELECT_POWER_SUPPLIER_FROM_DEVICE_INFO)
    if power:
        return power[0][0]
    else:
        return 0

def get_overall_power(db):
    overall_power = db.execute_query_with_output(SELECT_OVERALL_POWER_FROM_POWER_CONSUMPTION)
    logging.info(f"get_overallPower {overall_power}")
    if overall_power:
        return overall_power[0][0]
    else:
        return 0

def send_power_to_current_consumption_topic(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        current_power = get_overall_power(args[0].db_server)
        max_power = get_power_supplier_power(args[0].db_server)
        args[0].publish(topic='power_consumption_current', payload=json.dumps({"power": max_power, "consumption": current_power}), retain = True)
        return result
    return wrapper

def send_active_devices_topic(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        logging.info('Executing wrapper')
        query = f'SELECT PowerConsumption.DeviceId, SUM(PowerConsumption.DeviceConsumption) AS Cons, DevicesInfo.ServiceName, DevicesInfo.DeviceType, updated_services_name.new_name ' \
                f'FROM PowerConsumption ' \
                f'LEFT JOIN DevicesInfo ' \
                f'ON PowerConsumption.DeviceId = DevicesInfo.Id ' \
                f'LEFT JOIN updated_services_name ' \
                f'ON DevicesInfo.ServiceName = updated_services_name.service_name ' \
                f"GROUP BY DeviceId HAVING Cons > 0"
        active_devices = args[0].db_server.execute_query_with_output(query)
        logging.info(f"Active devices: {active_devices}")
        payload = []
        if active_devices:
            for device in active_devices:
                # query_for_name = f"SELECT new_name FROM updated_services_name WHERE service_name == '{device[2]}'"
                # service_new_name = args[0].db_server.execute_query_with_output(query_for_name)
                # [{"service_name": <str>, "power": <int>, "id": <int>}]
                payload.append({"service_name": device[2], "power": device[1], "name": device[3] if device[3] else device[2]})
                #payload = payload + '{' + f'"service_name": "{device[2]}", "power": {device[1]}' + '},'
        args[0].publish(topic='active_devices', payload = json.dumps(payload), retain = True)
        return result
    return wrapper

def send_discovered_devices_topic(func):
    def wrapper(*args, **kwargs):
        result = func(*args, **kwargs)
        query = f'SELECT DevicesInfo.Id, DevicesInfo.ServiceName, DevicesInfo.Power, DevicesInfo.DeviceType, updated_services_name.new_name, CASE WHEN DevicesInfo.IsConsumer THEN ActiveDevices.Cons ELSE 1 END FROM DevicesInfo LEFT JOIN updated_services_name ON updated_services_name.service_name = DevicesInfo.ServiceName ' \
                f' LEFT JOIN (SELECT PowerConsumption.DeviceId AS DeviceId, SUM(PowerConsumption.DeviceConsumption) AS Cons FROM PowerConsumption GROUP BY DeviceId HAVING Cons > 0) ActiveDevices ON ActiveDevices.DeviceId = DevicesInfo.Id'
        active_devices = args[0].db_server.execute_query_with_output(query)
        logging.info(f"Discovered devices: {active_devices}")
        payload = []
        if active_devices:
            for device in active_devices:
                payload.append({"service_name": device[1], "power": device[2], "name": device[3] if device[3] else device[1], "state": True if device[5] else False })
        args[0].publish(topic='discovered_devices', payload = json.dumps(payload), retain = True)
        logging.info(f'Discovered payload: {payload}')
        return result
    return wrapper

def add_to_be_on(to_be_on_id, who_off_id, db):
    db.insert_in_db('to_be_power_on', 'id_device_to_turned_on, id_device_who_turned_off', (
                to_be_on_id,
                who_off_id
                ))

def power_on_after_off(client, who_off_id, db):
    query = f'SELECT DevicesInfo.ServiceName, DevicesInfo.Power, DevicesInfo.Id, to_be_power_on.id_device_to_turned_on ' \
            f'FROM to_be_power_on LEFT JOIN DevicesInfo ON to_be_power_on.id_device_to_turned_on == DevicesInfo.Id ' \
            f"WHERE to_be_power_on.id_device_who_turned_off == {who_off_id}"
    
    devices_to_be_on = db.execute_query_with_output(query)
    if devices_to_be_on:
        for device in devices_to_be_on:
            # send message
            logging.info(f"Device power on: {device}")
            client.publish(topic=f"{device[0]}/set.from_server",
                                        payload=json.dumps({"command": "power_on", "parameters":{"state": True}}))
            # insert in db
            overall_power = get_overall_power(db)
            db.insert_in_db('PowerConsumption',
                           "DeviceId, DeviceConsumption, OverallConsumption",
                           (device[2],
                            device[1],
                            overall_power + device[1],
                           )
                           )
            query = f'SELECT Priority, IsActive, Blocked ' \
                    f'FROM DeviceState WHERE Id == {device[2]} ORDER BY Id LIMIT 1'
            result = db.execute_query_with_output(query)[0]
            db.insert_in_db('DeviceState', 'DeviceId, Priority, State, IsActive, Blocked', (
                            device[2],
                            result[0],
                            True,
                            result[1],
                            result[2]
                            ))
            db.delete_from_db(
                'to_be_power_on',
                f"id_device_to_turned_on == '{device[2]}'"
            )
            print(f'Removed device from the turn on list: {device[2]}')

def on_result(client, msg, avahi_server, db):
    service_name = msg.topic.split('/')[0]
    requester_id = client.last.get("id")
    on_request_result.append({"service_name": service_name, "requester_id": requester_id, "result":client.last.get("result")})

def wait_for_response(service_name, requester_id):
 #   print("We are in the waiting loop")
 #   for x in range (0,60):
 #       print(f"We are in the waiting loop: {x}")
 #       if on_request_result:
 #           if on_request_result[0].get("service_name") == service_name and on_request_result[0].get("requester_id") == requester_id and on_request_result[0].get("result"):
 #               return True
 #       await asyncio.sleep(1)
 #   on_request_result.clear()
    return True

@send_discovered_devices_topic
def registration_procedure(client, msg, avahi_server, db):
    service_name_from_topic = msg.topic.split('/')[0]
    logging.info(f'Service name: {service_name_from_topic}')
    logging.info(f'mqtt topic: {msg.topic.split("/")}')
    if db.get_all_from_db(table='DevicesInfo', condition=f"ServiceName == '{msg.topic.split('/')[0]}'"):
        client.publish(topic=f"{service_name_from_topic}/set.from_server", payload=json.dumps({"result": True, "parameters": {}}))
        return
    
    data = json.loads(msg.payload)
    print(data['parameters']['name'])
    if not db.insert_in_db('DevicesInfo', 'ServiceName, DeviceType, Power, IsConsumer', (
            data['parameters']['name'],
            data['parameters']['device'],
            data['parameters']['power'],
            data['parameters']['is_consumer']
    )):
        client.publish(topic=f"{service_name_from_topic}/set.from_server", payload=json.dumps({"result": False, "parameters": {}}))
        print('Inserting in DeviceInfo failed')
        return
    logging.info('************Getting static data************')
    static_service_db_data = db.get_all_from_db(table='DevicesInfo',
                                                condition=f"ServiceName == '{data['parameters']['name']}'")
    logging.info(f"After Getting static data: {static_service_db_data}")
    if data['parameters']['is_consumer']:
        logging.info('Is consumer True')
        logging.info('DeviceState insert')
        logging.info("*********** HERE IT IS *************\n\n\n")
        logging.info(f"******************** POWER SUPPLIER: {get_power_supplier_power(db)} **********************\n\n\n")
        print("**********HERE************")
        if static_service_db_data[0][3] <= get_power_supplier_power(db):
            is_active = True
            state = client.last.get('parameters').get('state')
        else:
            is_active = False
            state = False
        if not db.insert_in_db('DeviceState', 'DeviceId, Priority, State, IsActive, Blocked', (
                db.get_all_from_db(table='DevicesInfo',
                                   condition=f"ServiceName == '{data['parameters']['name']}'")[0][0],
                client.last.get('parameters').get('priority'),
                state,
                is_active,
                client.last.get('parameters').get('blocked'),
                )):
            print('DeviceState Table insert')
            client.publish(topic=f"{msg.topic.split('/')[0]}/set.from_server",
                           payload=json.dumps({"result": False, "parameters": []}))
        else:
            client.publish(topic=f"{msg.topic.split('/')[0]}/set.from_server",
                           payload=json.dumps({"result": True, "parameters": {"is_active": is_active}}))
    else:
        print('Is consumer False')
        print('DeviceState insert')
        # TODO check supplier state
        if not db.insert_in_db('DeviceState', 'DeviceId, Priority, State, IsActive, Blocked', (
                db.get_all_from_db(table='DevicesInfo',
                                   condition=f"ServiceName == '{data['parameters']['name']}'")[0][0],
                client.last.get('parameters').get('priority'),
                client.last.get('parameters').get('state'),
                client.last.get('parameters').get('is_active'),
                client.last.get('parameters').get('blocked')
        )):
            print('Cant DeviceState insert')
            client.publish(topic=f"{msg.topic.split('/')[0]}/set.from_server",
                           payload=json.dumps({"result": False, "parameters": {}}))
        else:
            client.publish(topic=f"{msg.topic.split('/')[0]}/set.from_server", payload=json.dumps({"result": True, "parameters": {}}))

@send_power_to_current_consumption_topic
@send_active_devices_topic
@send_discovered_devices_topic
def power_on_procedure(client, msg, avahi_server, db):
    service_name_from_topic = msg.topic.split('/')[0]

    blocked_state = msg.payload['parameters']['blocked']

    if blocked_state = True
	print("***********Device on power on blocked*****************")
	client.publish(topic=f"{service_name_from_topic}/set.from_server",
                       payload=json.dumps({"result": False, "parameters": {"blocked": True}}))
	return

    print("*********************POWER ON************************")
    static_service_db_data = db.get_all_from_db(table='DevicesInfo',
                                                condition=f"ServiceName == '{service_name_from_topic}'")
    if not static_service_db_data:
        print("Reject power on request: no available power")
        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                    payload=json.dumps({"result": False, "parameters": {"state": False}}))
        return
    dynamic_service_db_data = db.get_all_from_db(table='DeviceState',
                                                 condition=f"DeviceId == {static_service_db_data[0][0]}")
    if dynamic_service_db_data[-1][4] == 0:
        print('Device is not in is_active: True condition')
        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                       payload=json.dumps({"result": False, "parameters": {"state": False}}))
        return
    # TODO: Add veriffication for device state
    if dynamic_service_db_data[-1][3] == 1:
        print('Device is already power on')
        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                       payload=json.dumps({"result": False, "parameters": {"state": True}}))
        return
    overall_power = get_overall_power(db)
    max_power = get_power_supplier_power(db)
    available_overall_power = max_power - overall_power
    print(f"Available power:{available_overall_power}")
    print("*********************COMPLETED CHECK************************")
    print(static_service_db_data[0][3])
    print(available_overall_power)
    if static_service_db_data[0][3] > available_overall_power:
        # Free up power
        print("************************CHECK PRIORITY*************************")
        logging.info(f"Dynamic state: {dynamic_service_db_data[0][3]}")
        query = f'SELECT DevicesInfo.ServiceName, DeviceState.Priority, DeviceState.DeviceId, DevicesInfo.Power ' \
                f'FROM DevicesInfo LEFT JOIN DeviceState ON DevicesInfo.Id == DeviceState.DeviceId ' \
                f"WHERE DevicesInfo.IsConsumer == 1 and DeviceState.State == 1 AND DeviceState.Priority > {dynamic_service_db_data[-1][2]} ORDER BY DeviceState.Priority"
        result = db.execute_query_with_output(query)
        power_to_release = static_service_db_data[0][3] - available_overall_power
        devices_to_power_off = []
        logging.info(f"Result: {result}")
        print("*****************PASSED HERE**************")
        print(result)
        if result:
            print("*****************YES IT IS HERE**************")
            for record in result:
                if power_to_release <= 0:
                    break
                devices_to_power_off.append({"id":record[2], "name":record[0], "power":record[3]})
                logging.info(f"Record :{record}")
                power_to_release = power_to_release - int(record[3])
            logging.info(f"devices_to_power_off: {devices_to_power_off}")
            if devices_to_power_off:
                for device in devices_to_power_off:
                    logging.info(f"Device: {device}")
                    client.publish(topic=f"{device.get('name')}/set.from_server",
                                    payload=json.dumps({"command": "power_off", "id": static_service_db_data[0][0], "parameters": {"state": False}}))
                    logging.info(f"The message has been sent to the device: {device}")
                    overall_power = get_overall_power(db)
                    if wait_for_response(device.get('name'), static_service_db_data[0][0]):
                        db.insert_in_db('PowerConsumption',
                                        "DeviceId, DeviceConsumption, OverallConsumption",
                                        (device.get("id"),
                                        device.get("power") * -1,
                                        overall_power - device.get("power"),
                                        )
                                        )
                        add_to_be_on(device.get("id"), static_service_db_data[0][0], db)
                    else:
                        print("Reject power on request: device has not respond")
                        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                                payload=json.dumps({"result": False, "parameters": {"state": False}}))
                        return
            else:
                print("Reject power on request: no available power the list of the devices is empty")
                client.publish(topic=f"{service_name_from_topic}/set.from_server",
                            payload=json.dumps({"result": False, "parameters": {"state": False}}))
                return
        else:
            print("Reject power on request: no available power")
            client.publish(topic=f"{service_name_from_topic}/set.from_server",
                        payload=json.dumps({"result": False, "parameters": {"state": False}}))
            return
        
    overall_power = get_overall_power(db)
    db.insert_in_db('PowerConsumption',
                    "DeviceId, DeviceConsumption, OverallConsumption",
                    (
                        static_service_db_data[0][0],
                        static_service_db_data[0][3],
                        overall_power + static_service_db_data[0][3]
                    )
                    )
    logging.info('Inserted new record in PowerConsumption')
    db.insert_in_db('DeviceState', "DeviceId, Priority, State, IsActive, Blocked",
                    (
                        static_service_db_data[0][0],
                        client.last.get('parameters').get('priority'), 
                        client.last.get('parameters').get('state'),
                        client.last.get('parameters').get('is_active'),
                        client.last.get('parameters').get('blocked')
                        )
                        )
    client.publish(topic=f"{service_name_from_topic}/set.from_server", payload=json.dumps({"result": True, "parameters": {"state": True}}))
    logging.info('Response published')

@send_power_to_current_consumption_topic
@send_active_devices_topic
@send_discovered_devices_topic
def power_off_procedure(client, msg, avahi_server, db):
    service_name_from_topic = msg.topic.split('/')[0]

    blocked_state = msg.payload['parameters']['blocked']

    if blocked_state = True
        print("***********Device on power on blocked*****************")
        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                       payload=json.dumps({"result": False, "parameters": {"blocked": False}}))
        return

    service_db_data = db.get_all_from_db(table='DevicesInfo',
                                             condition=f"ServiceName == '{service_name_from_topic}'")
    service_db_data = service_db_data[0] if service_db_data else None
    print(service_db_data)
    query = f'SELECT State FROM DeviceState WHERE DeviceId == {service_db_data[0] if service_db_data else None} ORDER BY Id DESC LIMIT 1'
    state = db.execute_query_with_output(query)
    state = state[0][0] if state else None
    print(service_db_data[0])
    if state == 1:
        overall_power = get_overall_power(db)
        db.insert_in_db('PowerConsumption',
                        "DeviceId, DeviceConsumption, OverallConsumption",
                        (
                            service_db_data[0],
                            service_db_data[3] * -1,
                            overall_power - service_db_data[3]
                        )
                        )
        if db.insert_in_db('DeviceState', "DeviceId, Priority, State, IsActive, Blocked",
                            (service_db_data[0],
                            client.last.get('parameters').get('priority'),
                            False,
                            client.last.get('parameters').get('is_active'),
                            client.last.get('parameters').get('blocked'))):
            power_on_after_off(client, service_db_data[0], db)
            client.publish(topic=f"{service_name_from_topic}/set.from_server",
                           payload=json.dumps({"result": True, "parameters": {"state": False}}))
        else:
            client.publish(topic=f"{service_name_from_topic}/set.from_server", payload=json.dumps({"result": False, "parameters": {}}))
    else:
        logging.info('Device is already power off')
        client.publish(topic=f"{service_name_from_topic}/set.from_server",
                       payload=json.dumps({"result": False, "parameters": {}}))

def get_status_procedure(client, msg, avahi_server, db):
    service_name_from_topic = msg.topic.split('/')[0]
    service_data = avahi_server.services_discovered.get(service_name_from_topic)
    if not service_data:
        logging.info("Device is not registered on avahi")
        return
    static_service_db_data = db.get_all_from_db(table='DevicesInfo',
                                                condition=f"ServiceName == '{service_data.name}'")
    if not static_service_db_data:
        logging.info("Device is not registered yet")
        return
    query = f'SELECT Priority, IsActive, Blocked, State ' \
                    f'FROM DeviceState WHERE DeviceId == {static_service_db_data[0][0]} ORDER BY Id DESC LIMIT 1'
    dynamic_status = db.execute_query_with_output(query)
    query_for_name = f"SELECT new_name FROM updated_services_name WHERE service_name == '{service_data.name}'"
    service_new_name = db.execute_query_with_output(query_for_name)
    if dynamic_status:
        logging.info(f'Respons on get status request: {dynamic_status}')
        # {"priority": 3,"is_active": true, "blocked": true, "state": true}
        param = {}
        param["priority"] = dynamic_status[0][0]
        param["is_active"] = True if dynamic_status[0][1] == 1 else False
        param["blocked"] = True if dynamic_status[0][2] == 1 else False
        param["state"] = True if dynamic_status[0][3] == 1 else False
        param["name"] = service_new_name[0][0] if service_new_name else service_data.name
        data = {}
        data["result"] = True
        data["parameters"] = param
        client.publish(topic=f"{service_name_from_topic}/get.from_server",
                        payload=json.dumps(data))


def update_procedure(client, msg, avahi_server, db):
    service_name_from_topic = msg.topic.split('/')[0]
    #service_data = avahi_server.services_discovered.get(service_name_from_topic)
    #if not service_data:
    #    logging.info("Device is not registered on avahi")
    #    return
    static_service_db_data = db.get_all_from_db(table='DevicesInfo',
                                                condition=f"ServiceName == '{service_data.name}'")
    if not static_service_db_data:
        logging.info("Device is not registered yet")
        return
    query = f'SELECT Priority, IsActive, Blocked, State ' \
            f'FROM DeviceState WHERE DeviceId == {static_service_db_data[0][0]} ORDER BY Id DESC LIMIT 1'
    result = db.execute_query_with_output(query)
    if result:
        DeviceId = static_service_db_data[0][0]
        res = client.last.get('parameters').get('priority')
        if res:
            Priority = res
        else:
            Priority = result[0][0]
        IsActive = result[0][1]
        res = client.last.get('parameters').get('blocked')
        logging.info(f'Data from mqtt: {res}')
        if res is not None:
            Blocked = res
        else:
            logging.info(f'Data from sql: {result[0][2]}')
            Blocked = result[0][2]
        State = result[0][3]
        query = f'INSERT INTO DeviceState (DeviceId, Priority, IsActive, Blocked, State) VALUES ' \
                    f'({DeviceId}, {Priority}, {IsActive}, {Blocked}, {State})'
        result = db.execute_query_without_output(query)
    data = {}
    data["result"] = True
    data["parameters"] = client.last.get('parameters')
    client.publish(topic=f"{service_name_from_topic}/set.from_server",
                    payload=json.dumps(data))

@send_active_devices_topic
@send_discovered_devices_topic
def new_name_procedure(client, msg, db):
    service_name_from_topic = msg.topic.split('/')[0]
    new_service_name = client.last.get('parameters').get('name')
    query = f"UPDATE DevicesInfo SET DeviceType = '{new_service_name}' WHERE ServiceName = '{service_name_from_topic}'"
    result = db.execute_query_without_output(query)
    client.publish(topic=f"{service_name_from_topic}/set.from_server",
                   payload=json.dumps({"result": result, "parameters": {"name": new_service_name}}))

def set_commands_handler(client, msg, avahi_server, db):
    print(f"****** COMMAND CODE: {client.last.get('command')} ********")
    if client.last.get("command") == "register":
        logging.info('**********Executing registration procedure*****************')
        registration_procedure(client, msg, avahi_server, db)
    elif client.last.get("command") == "power_on":
        logging.info('Execution power on procedure')
        power_on_procedure(client, msg, avahi_server, db)
    elif client.last.get("command") == "power_off":
        logging.info('Executing power off procedure')
        power_off_procedure(client, msg, avahi_server, db)
    elif client.last.get("command") == "update" and next(iter(client.last.get("parameters"))) == "name":
        logging.info('Updating service name')
        new_name_procedure(client, msg, db)
    elif client.last.get("command") == "update":
        logging.info('Executing update procedure')
        update_procedure(client, msg, avahi_server, db)
    elif client.last.get("result"):
        logging.info('Executing on result procedure')
        on_result(client, msg, avahi_server, db)
    else:
        pass

def get_commands_handler(client, msg, avahi_server, db):
    if client.last.get("command") == "get_status":
        logging.info('Executing get status procedure')
        get_status_procedure(client, msg, avahi_server, db)
    else:
        pass
