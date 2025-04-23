crops = [
    "peanut",
    "sugar_cane",
    "cassava",
    "sweet_potatoes",
    "corn",
    "paddy",
]

config_minio = {
    "endpoint": "http://localhost:8999",
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


def get_selected_items_faostat():
    return selected_items


def get_crops_gos():
    return crops


def get_config_minio():
    return config_minio
