import logging
from data_manager import DataManager

def main():
    try:
        entsoe = DataManager(schema="HUN", local_timezone='CET')
        entsoe.update_power_prices()
        entsoe.update_activated_balancing_energy()
        entsoe.update_fuelmix()
    
    except Exception as e:
        logging.error(f"Error: {e}")
        raise e
  
if __name__ == "__main__":
    main()