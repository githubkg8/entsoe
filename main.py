from data_manager import DataManager

entsoe=DataManager(schema="HUN",local_timezone='CET')

entsoe.update_power_prices()
entsoe.update_activated_balancing_energy()