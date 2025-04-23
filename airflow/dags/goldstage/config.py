config_minio = {
    "endpoint": "http://host.docker.internal:8999",
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

province = {
    "Hà Nội": "Ha Noi",
    "Hải Phòng": "Hai Phong",
    "Quảng Ninh": "Quang Ninh",
    "Bắc Giang": "Bac Giang",
    "Bắc Kạn": "Bac Kan",
    "Bắc Ninh": "Bac Ninh",
    "Cao Bằng": "Cao Bang",
    "Điện Biên": "Dien Bien",
    "Hà Giang": "Ha Giang",
    "Hà Nam": "Ha Nam",
    "Hòa Bình": "Hoa Binh",
    "Hưng Yên": "Hung Yen",
    "Lai Châu": "Lai Chau",
    "Lào Cai": "Lao Cai",
    "Lạng Sơn": "Lang Son",
    "Nam Định": "Nam Dinh",
    "Ninh Bình": "Ninh Binh",
    "Phú Thọ": "Phu Tho",
    "Sơn La": "Son La",
    "Thái Bình": "Thai Binh",
    "Thái Nguyên": "Thai Nguyen",
    "Tuyên Quang": "Tuyen Quang",
    "Vĩnh Phúc": "Vinh Phuc",
    "Yên Bái": "Yen Bai",
    "Hải Dương": "Hai Duong",
    "Đà Nẵng": "Da Nang",
    "Thừa Thiên Huế": "Thua Thien Hue",
    "Quảng Trị": "Quang Tri",
    "Quảng Bình": "Quang Binh",
    "Hà Tĩnh": "Ha Tinh",
    "Nghệ An": "Nghe An",
    "Thanh Hóa": "Thanh Hoa",
    "Quảng Nam": "Quang Nam",
    "Quảng Ngãi": "Quang Ngai",
    "Bình Định": "Binh Dinh",
    "Phú Yên": "Phu Yen",
    "Khánh Hòa": "Khanh Hoa",
    "Ninh Thuận": "Ninh Thuan",
    "Bình Thuận": "Binh Thuan",
    "Hồ Chí Minh": "Ho Chi Minh",
    "Bà Rịa - Vũng Tàu": "Ba Ria - Vung Tau",
    "Bình Dương": "Binh Duong",
    "Bình Phước": "Binh Phuoc",
    "Đồng Nai": "Dong Nai",
    "Tây Ninh": "Tay Ninh",
    "An Giang": "An Giang",
    "Bạc Liêu": "Bac Lieu",
    "Bến Tre": "Ben Tre",
    "Cà Mau": "Ca Mau",
    "Cần Thơ": "Can Tho",
    "Đồng Tháp": "Dong Thap",
    "Hậu Giang": "Hau Giang",
    "Kiên Giang": "Kien Giang",
    "Long An": "Long An",
    "Sóc Trăng": "Soc Trang",
    "Tiền Giang": "Tien Giang",
    "Trà Vinh": "Tra Vinh",
    "Vĩnh Long": "Vinh Long",
    "Lâm Đồng": "Lam Dong",
    "Gia Lai": "Gia Lai",
    "Kon Tum": "Kon Tum",
    "Đắk Lắk": "Dak Lak",
    "Đắk Nông": "Dak Nong"
}


code_crops = [('cassava', 125), ('paddy', 27), ('corn', 56),
              ('peanut', 242), ('sugar_cane', 156), ('sweet_potatoes', 122)]


def get_selected_items_faostat():
    return selected_items


def get_code_crops():
    return code_crops


def get_province():
    return province


def get_config_minio():
    return config_minio
