

parameter_url = {
    "peanut":
    {
        "production": "https://pxweb.gso.gov.vn/sq/f18602e3-3c33-4a45-923d-8983eb304262",
        "planted_area": "https://pxweb.gso.gov.vn/sq/adbf69fa-da47-4d1f-91c4-52783633b0c4",
        "yield": None
    },
    "sugar_cane":
    {
        "production": "https://pxweb.gso.gov.vn/sq/aa33020d-276e-474c-9a8a-d43ba34e09a1",
        "planted_area": "https://pxweb.gso.gov.vn/sq/c13ecc6e-5c88-41e0-853b-fa384a9d4ba9",
        "yield": None
    },
    "cassava":
    {
        "production": "https://pxweb.gso.gov.vn/sq/dbde7667-9e84-410c-8711-541333b35b0c",
        "planted_area": "https://pxweb.gso.gov.vn/sq/0e21dec5-224f-416f-8b68-ec7a6312c807",
        "yield": None
    },
    "sweet_potatoes":
    {
        "production": "https://pxweb.gso.gov.vn/sq/bb7f3e3f-8859-4094-aeb8-92a997dd5737",
        "planted_area": "https://pxweb.gso.gov.vn/sq/39bf26c2-d8a7-4eb2-8ada-e46f58a949a3",
        "yield": None
    },
    "corn":
    {
        "production": "https://pxweb.gso.gov.vn/sq/a814a4c7-bb44-4f1a-b09a-1f3421533cc2",
        "planted_area": "https://pxweb.gso.gov.vn/sq/f01be30e-98bf-4a3b-941c-defd8d7c340d",
        "yield": "https://pxweb.gso.gov.vn/sq/4b0e915c-4aef-41af-afea-e57b3fe58478"
    },
    "paddy":
    {
        "production": "https://pxweb.gso.gov.vn/sq/1eb94393-0e1e-464a-bce0-cfa9945ceebc",
        "planted_area": "https://pxweb.gso.gov.vn/sq/071faf34-14bd-4c4e-83dc-d8cfcb08ce6b",
        "yield": "https://pxweb.gso.gov.vn:443/sq/ea495f5c-efe7-4e76-bc29-cd1f39f9a9db"
    }
}

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

config_minio = {
    "endpoint": "http://localhost:8999",
    "access_key": "minioadmin",
    "secret_key": "minioadmin",
}

config_weather = {
    "weatherapi": {
        "base_url": "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/",
        "api_key": [
            "URY5J8GBA74X6LQSSVKW6TXPF",
            "KUX7JH47KH5SJZVBZDAP3RSQC",
            "NYG9SB5AKNYBCXF7CZ9FA3C95",
            "2YE4MG8WMKQF774E4MSVPWTYG",
            "PR976WBKEMXM5JXWHZBR2HR3X",
            "ENSKPNRLN5KPFX32KHMPNWV3X"
        ],
        "apI_key_used": []
    }
}

parameter_sets = [
    {"domain_code": "QCL", "record_id_cols": [
        "ElementCode", "ItemCode", "YearCode"], "table_name": "production_clp"},
    {"domain_code": "QI",  "record_id_cols": [
        "ElementCode", "ItemCode", "YearCode"], "table_name": "production_pi"},
    {"domain_code": "QV",  "record_id_cols": [
        "ElementCode", "ItemCode", "YearCode"], "table_name": "production_vap"},
    {"domain_code": "TCL", "record_id_cols": [
        "ElementCode", "ItemCode", "YearCode"], "table_name": "trade_clp"},
    {"domain_code": "PP",  "record_id_cols": [
        "ElementCode", "ItemCode", "YearCode", "MonthsCode"], "table_name": "price_pp"}
]


def get_parameter_sets_faostat():
    return parameter_sets


def get_url_gos():
    return parameter_url


def get_config_minio():
    return config_minio


def get_province():
    return province


def get_weather_baseurl():
    return config_weather["weatherapi"]["base_url"]


def get_api_key_weather():
    return config_weather["weatherapi"]["api_key"]
