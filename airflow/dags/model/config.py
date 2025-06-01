config_minio = {
    "endpoint": "http://host.docker.internal:9000",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

selected_items = [
    "Bananas", "Beans, dry", "Cabbages", "Cashew nuts, in shell", "Cassava, fresh",
    "Cauliflowers and broccoli", "Chillies and peppers, dry (Capsicum spp., Pimenta spp.), raw",
    "Coconuts, in shell", "Coffee, green", "Grapes", "Groundnuts, excluding shelled",
    "Maize (corn)", "Mangoes, guavas and mangosteens", "Mushrooms and truffles",
    "Onions and shallots, dry (excluding dehydrated)", "Oranges", "Papayas",
    "Pepper (Piper spp.), raw", "Pineapples", "Pomelos and grapefruits", "Potatoes",
    "Rice", "Sesame seed", "Soya beans", "Sugar cane", "Sweet potatoes", "Tea leaves",
    "Watermelons", "Apples", "Avocados", "Carrots and turnips", "Cashew nuts, shelled",
    "Chillies and peppers, green", "Coconuts, desiccated", "Cucumbers and gherkins",
    "Eggplants (aubergines)", "Ginger, raw", "Green garlic", "Groundnuts, shelled",
    "Leeks and other alliaceous vegetables", "Pears", "Plums and sloes", "Plums, dried",
    "Soybeans", "Strawberries", "Tangerines, mandarins, clementines", "Tomatoes", "Yams"
]

hyperparameters = {
    "learning_rate": 0.001,
    "num_lstm_layer": [1 ,2, 3],
    "units":[32, 64, 128],
    "dropout": 0.1,
    "sequence_length": 6,
    "sequence_lengths": [6, 12],
    "activation": 'tanh',
    "optimizer": "adam",
    "batch_sizes" : [32, 64],
    "epoch_list" : [100, 200]
}

all_crop_codes = [217,560,156,507,56,125,574,176,571,667,116,122,393,656,
    27,687,567,358,490,289,242,236,403]

def get_config_minio():
    return config_minio

def get_selected_items_faostat():
    return selected_items

def get_hyperparameters():
    return hyperparameters

def get_all_crop_codes():
    return all_crop_codes