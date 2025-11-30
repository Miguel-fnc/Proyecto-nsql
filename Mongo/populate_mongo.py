#!/usr/bin/env python3
import csv
import requests

BASE_URL = "http://localhost:8000"

def load_csv(path):
    with open(path, encoding="utf-8") as fd:
        return list(csv.DictReader(fd))


def post_many(endpoint, records):
    url = f"{BASE_URL}/{endpoint}"
    print(f"\nEnviando datos a: {url}")

    for rec in records:
        # Limpia campos vacíos y convierte a tipos adecuados si se requiere
        rec = {k: v for k, v in rec.items() if v != ""}

        r = requests.post(url, json=rec)

        if not r.ok:
            print(f"Error posting to {endpoint}: {r.status_code} - {r.text}")
        else:
            print(f"Insertado en {endpoint}: {r.json()}")


def main():
    # 1. Categories
    try:
        categories = load_csv("data/categories.csv")
        post_many("categories", categories)
    except FileNotFoundError:
        print("categories.csv no encontrado.")

    # 2. Brands
    try:
        brands = load_csv("data/brands.csv")
        post_many("brands", brands)
    except FileNotFoundError:
        print("brands.csv no encontrado.")

    # 3. Products
    try:
        products = load_csv("data/products.csv")

        # transforma campos especiales
        for p in products:
            if "price" in p:
                p["price"] = float(p["price"])

        post_many("products", products)
    except FileNotFoundError:
        print("products.csv no encontrado.")

    # 4. Users
    try:
        users = load_csv("data/users.csv")
        post_many("users", users)
    except FileNotFoundError:
        print("users.csv no encontrado.")

    # 5. Orders
    try:
        orders = load_csv("data/orders.csv")
        post_many("orders", orders)
    except FileNotFoundError:
        print("orders.csv no encontrado.")

    # 6. Promotions
    try:
        promotions = load_csv("data/promotions.csv")
        post_many("promotions", promotions)
    except FileNotFoundError:
        print("promotions.csv no encontrado.")


if __name__ == "__main__":
    main()
