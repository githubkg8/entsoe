import logging
from data_manager import DataManager

def main():
    try:
        entsoe_H = DataManager(schema="HUN", local_timezone='CET')
        entsoe_H.update_power_prices()
        entsoe_H.update_activated_balancing_energy()
        entsoe_H.update_fuelmix()
        entsoe_H.update_actual_total_load()
    
        entsoe_D = DataManager(schema="GER", local_timezone='CET')
        entsoe_D.update_power_prices()
    except Exception as e:
        logging.error(f"Error: {e}")
        raise e
  
if __name__ == "__main__":
    main()