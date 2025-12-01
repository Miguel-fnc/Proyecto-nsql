#!/usr/bin/env python3
import os
import sys
import subprocess

def print_menu():
    options = {
        1: "Listar todos los productos",
        2: "Listar todos los usuarios",
        3: "Listar todas las órdenes",
        4: "Listar categorías",
        5: "Listar marcas",
        6: "Listar promociones",
        7: "Buscar productos por nombre",
        8: "Buscar productos por categoría",
        9: "Buscar productos por rango de precio",
        10: "Búsqueda Avanzada (combinada)",
        11: "Regresar al menú principal"
    }

    print("\n------ MENU MONGODB------")
    for key, value in options.items():
        print(f"{key}. {value}")
    print("----------------------------------------------\n")


def mongo_menu():
    print("\n==== MODULO MONGODB ====\n")

    PY = sys.executable
    CLIENT = os.path.join(os.path.dirname(__file__), "client.py")

    while True:
        print_menu()
        opt = input("Selecciona una opción: ").strip()

        if not opt.isdigit():
            print("Opción inválida.\n")
            continue

        opt = int(opt)

        # Listados Simples
        if opt == 1:
            subprocess.run([PY, CLIENT, "list_products"])

        elif opt == 2:
            subprocess.run([PY, CLIENT, "list_users"])

        elif opt == 3:
            subprocess.run([PY, CLIENT, "list_orders"])

        elif opt == 4:
            subprocess.run([PY, CLIENT, "categories"])

        elif opt == 5:
            subprocess.run([PY, CLIENT, "brands"])

        elif opt == 6:
            subprocess.run([PY, CLIENT, "promotions"])

        # Búsquedas Básicas
        elif opt == 7:
            name = input("Nombre del producto: ").strip()
            subprocess.run([PY, CLIENT, "list_products", "--name", name])

        elif opt == 8:
            cat = input("Categoría: ").strip()
            subprocess.run([PY, CLIENT, "list_products", "--category", cat])

        elif opt == 9:
            min_p = input("Precio mínimo: ")
            max_p = input("Precio máximo: ")
            subprocess.run([
                PY, CLIENT, "list_products",
                "--min_price", min_p,
                "--max_price", max_p
            ])

        # Búsqueda Avanzada
        elif opt == 10:
            print("\n--- BÚSQUEDA AVANZADA ---")
            name = input("Nombre (enter para omitir): ").strip()
            cat = input("Categoría (enter para omitir): ").strip()
            min_p = input("Precio mínimo (enter para omitir): ").strip()
            max_p = input("Precio máximo (enter para omitir): ").strip()
            limit = input("Limit (enter para default 20): ").strip()
            skip = input("Skip (enter para default 0): ").strip()

            cmd = [PY, CLIENT, "list_products"]

            if name: cmd += ["--name", name]
            if cat: cmd += ["--category", cat]
            if min_p: cmd += ["--min_price", min_p]
            if max_p: cmd += ["--max_price", max_p]
            if limit: cmd += ["--limit", limit]
            if skip: cmd += ["--skip", skip]

            subprocess.run(cmd)

        elif opt == 11:
            print("\nRegresando al menú principal...\n")
            return

        else:
            print("Opción inválida.\n")
