base_path_bronze = "s3a://bronze"
base_path_silver = "s3a://silver"
base_path_gold = "s3a://gold/warehouse"

Faostat_bronze_path = f"{base_path_bronze}/faostat_data"
Faostat_silver_path = f"{base_path_silver}/faostat_data"
Faostat_gold_path = f"{base_path_gold}/fact_faostat"

GSO_bronze_path = f"{base_path_bronze}/gso_data"
GSO_silver_path = f"{base_path_silver}/gso_data"
GSO_gold_path = f"{base_path_gold}/fact_yield"

Weather_bronze_path = f"{base_path_bronze}/weather_data"
Weather_silver_path = f"{base_path_silver}/weather_data"
Weather_gold_path = f"{base_path_gold}/fact_weather"


def Faostat_paths():
    return {
        "Faostat_bronze_path": Faostat_bronze_path,
        "Faostat_silver_path": Faostat_silver_path,
        "Faostat_gold_path": Faostat_gold_path
    }


def GSO_paths():
    return {
        "GSO_bronze_path": GSO_bronze_path,
        "GSO_silver_path": GSO_silver_path,
        "GSO_gold_path": GSO_gold_path
    }


def Weather_paths():
    return {
        "Weather_bronze_path": Weather_bronze_path,
        "Weather_silver_path": Weather_silver_path,
        "Weather_gold_path": Weather_gold_path
    }
