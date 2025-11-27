#!/usr/bin/env python3
import argparse
import logging
import os
import requests
import json

# Logger
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.FileHandler('ecommerce.log')
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
log.addHandler(handler)

# API base
API_URL = os.getenv("ECOM_API_URL", "http://localhost:8000")

# Print
def print_doc(title, doc):
    print("\n" + "="*60)
    print(f" {title}")
    print("="*60)
    for k, v in doc.items():
        print(f"{k}: {v}")
    print("="*60 + "\n")

# Products
def list_products(name=None, category=None, min_price=None, max_price=None, limit=20, skip=0):
    endpoint = f"{API_URL}/products"
    params = {}

    if name: params["name"] = name
    if category: params["category"] = category
    if min_price is not None: params["min_price"] = min_price
    if max_price is not None: params["max_price"] = max_price
    params["limit"] = limit
    params["skip"] = skip

    r = requests.get(endpoint, params=params)
    if r.ok:
        docs = r.json()
        print(f"\nTotal products: {len(docs)}\n")
        for d in docs:
            print_doc("Product", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def get_product(pid):
    endpoint = f"{API_URL}/products/{pid}"
    r = requests.get(endpoint)
    if r.ok:
        print_doc("Product Detail", r.json())
    else:
        print(f"Error: {r.status_code} {r.text}")



# Users
def list_users():
    endpoint = f"{API_URL}/users"
    r = requests.get(endpoint)
    if r.ok:
        docs = r.json()
        print(f"\nTotal users: {len(docs)}\n")
        for d in docs:
            print_doc("User", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def get_user(uid):
    endpoint = f"{API_URL}/users/{uid}"
    r = requests.get(endpoint)
    if r.ok:
        print_doc("User Detail", r.json())
    else:
        print(f"Error: {r.status_code} {r.text}")

# Orders
def list_orders():
    endpoint = f"{API_URL}/orders"
    r = requests.get(endpoint)
    if r.ok:
        docs = r.json()
        print(f"\nTotal orders: {len(docs)}\n")
        for d in docs:
            print_doc("Order", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def get_order(oid):
    endpoint = f"{API_URL}/orders/{oid}"
    r = requests.get(endpoint)
    if r.ok:
        print_doc("Order Detail", r.json())
    else:
        print(f"Error: {r.status_code} {r.text}")



# Collections
def list_categories():
    endpoint = f"{API_URL}/categories"
    r = requests.get(endpoint)
    if r.ok:
        for d in r.json():
            print_doc("Category", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def list_brands():
    endpoint = f"{API_URL}/brands"
    r = requests.get(endpoint)
    if r.ok:
        for d in r.json():
            print_doc("Brand", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def list_promotions():
    endpoint = f"{API_URL}/promotions"
    r = requests.get(endpoint)
    if r.ok:
        for d in r.json():
            print_doc("Promotion", d)
    else:
        print(f"Error: {r.status_code} {r.text}")


def main():

    actions = [
        "list_products", "get_product",
        "list_users", "get_user",
        "list_orders", "get_order",
        "categories", "brands", "promotions"
    ]

    parser = argparse.ArgumentParser()
    parser.add_argument("action", choices=actions)

    # ID
    parser.add_argument("-i", "--id", help="Document ID", default=None)

    # Product filters
    parser.add_argument("--name", default=None)
    parser.add_argument("--category", default=None)
    parser.add_argument("--min_price", type=float, default=None)
    parser.add_argument("--max_price", type=float, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--skip", type=int, default=0)

    args = parser.parse_args()

    if args.action == "list_products":
        list_products(args.name, args.category, args.min_price,
                      args.max_price, args.limit, args.skip)

    elif args.action == "get_product" and args.id:
        get_product(args.id)

    elif args.action == "list_users":
        list_users()

    elif args.action == "get_user" and args.id:
        get_user(args.id)

    elif args.action == "list_orders":
        list_orders()

    elif args.action == "get_order" and args.id:
        get_order(args.id)

    elif args.action == "categories":
        list_categories()

    elif args.action == "brands":
        list_brands()

    elif args.action == "promotions":
        list_promotions()

    else:
        print("Missing or invalid options.")


if __name__ == "__main__":
    main()
