SELECT_POWER_SUPPLIER_FROM_DEVICE_INFO = \
    'SELECT DevicesInfo.Power from DevicesInfo LEFT JOIN DeviceState ON DevicesInfo.Id == DeviceState.DeviceId ' \
    'WHERE DevicesInfo.IsConsumer == false AND DeviceState.State == true'
SELECT_OVERALL_POWER_FROM_POWER_CONSUMPTION = 'SELECT OverallConsumption FROM PowerConsumption ORDER BY Id DESC LIMIT 1'

