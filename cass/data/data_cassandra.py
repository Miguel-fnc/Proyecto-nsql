import uuid

NAMESPACE_BASE = uuid.UUID('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11') 

def generate_uuid(name):
    return uuid.uuid5(NAMESPACE_BASE, name)

PRODUCTS = [
    ("Futbol", "Taquetes Hypervenom Nike", 2500.00),
    ("Nutricion Deportiva", "Proteina Whey Gold Standard ON", 1199.50),
    ("Running", "Playera Dri-FIT manga corta color negro Puma", 679.00),
    ("Electronica Deportiva", "Reloj GPS multideporte Garmin Fenix 7", 13500.00),
    ("Accesorios Deportivos", "Balon de futbol Adidas Champions League", 899.00)
]
USERS = [
    ("miguel franco"),
    ("angel aceves"),
    ("karen santana"),
    ("omar madriz"),
    ("abraham hernandez"),
    ("juan pablo perez"),
    ("emiliano villagran")
]
BRANDS = [
    ("Adidas"),
    ("Nike"),
    ("Garmin"),
    ("Puma"),
    ("Under Armour"),
    ("Optimum Nutrition"),
    ("New Balance")
]
SEARCH_TERMS = [
    ("zapatos de fútbol", "Fútbol"),
    ("suplementos deportivos", "Nutrición Deportiva"),
    ("ropa deportiva", "Running"),
    ("relojes deportivos", "Electrónica Deportiva"),
    ("balones", "Accesorios Deportivos"),
    ("tenis para correr", "Running"),
    ("jerseys", "Fútbol")
]
ADS = [
    ("Pinta con confianza pinta con berel"),
    ("Trikitrakatelas, galletas gamesa"),
    ("Pinta tu ralla con comex"),
    ("Tarjeta BBVA"),
    ("Juguetes Mi Alegria")
]
ACTION_TYPE = [
    ("on_click"),
    ("Scroll"),
    ("Via e-mail")
]
ERRORS = [
    ("login", "email o contrasena invalida"),
    ("compra", "numero de cuenta invalido"),
    ("compra", "pago rechazado"),
    ("Stock", "producto no disponible")
]
PROMOTION_TYPE = [
    ("holiday promotion"),
    ("fathers day promotion"),
    ("mothers day promotion"),
    ("kids day promotion")
]

USER_IDS = {user: generate_uuid(user) for user in USERS}
PROMOTIONS_IDS = {promotion: generate_uuid(promotion) for promotion in PROMOTION_TYPE}