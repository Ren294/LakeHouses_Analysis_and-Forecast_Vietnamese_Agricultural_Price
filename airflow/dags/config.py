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

Dim_Date_path = f"{base_path_gold}/dim_date"
Dim_Crops_path = f"{base_path_gold}/dim_crops"
Dim_Province_path = f"{base_path_gold}/dim_province"

faostat_paths = {
    "Faostat_bronze_path": Faostat_bronze_path,
    "Faostat_silver_path": Faostat_silver_path,
    "Faostat_gold_path": Faostat_gold_path
}

gso_paths = {
    "GSO_bronze_path": GSO_bronze_path,
    "GSO_silver_path": GSO_silver_path,
    "GSO_gold_path": GSO_gold_path
}

weather_paths = {
    "Weather_bronze_path": Weather_bronze_path,
    "Weather_silver_path": Weather_silver_path,
    "Weather_gold_path": Weather_gold_path
}

dim_paths = {
    "Dim_Date_path": Dim_Date_path,
    "Dim_Crops_path": Dim_Crops_path,
    "Dim_Province_path": Dim_Province_path
}

predict_paths = {
    "Predict_Crop_Price_path": "s3a://predict//predict_crop_price"
}

all_crop_codes = [217,560,156,507,56,125,574,176,571,667,116,122,393,656,
    27,687,567,358,490,289,242,236,403,486,449]

[486, 449, 689, 249]
def Faostat_paths():
    return faostat_paths


def GSO_paths():
    return gso_paths


def Weather_paths():
    return weather_paths

def Dim_paths():
    return dim_paths

def Predict_paths():
    return predict_paths

def get_all_crop_codes():
    return all_crop_codes
