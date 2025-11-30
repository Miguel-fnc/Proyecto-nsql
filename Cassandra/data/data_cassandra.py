import uuid

def generate_uuid():
    return uuid.uuid4()


PRODUCTS = [
    ("deporte", "taquetes hypervenom nike", 2500),
    ("salud","creatina herbalife", 399.9),
    ("moda","playera manga corta color negro", 679),
    ("electronica","iphone 17", 35000),
    ("accesorios","reloj tissot plateado fondo verde", 5899)
]
USERS = [
    ("miguel franco"),
    ("angel aceves"),
    ("karen torres"),
    ("omar madriz"),
    ("abraham hernandez"),
    ("juan pablo perez"),
    ("emiliano villagran")
]
BRANDS = [
    ("adidas"),
    ("nike"),
    ("apple"),
    ("H&M"),
    ("Victus"),
    ("herbalife"),
    ("Tissot")
]
SEARCH_TERMS = [
    ("relojes", "accesorios"),
    ("ropa", "moda"),
    ("suplementos", "salud"),
    ("videojuegos", "videojuegos"),
    ("Celulares", "electronica"),
    ("computadora", "electronica"),
    ("jerseys", "deporte")
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
USER_IDS = {user: generate_uuid() for user in USERS}
PROMOTIONS_IDS = {promotion: generate_uuid() for promotion in PROMOTION_TYPE}