import csv
import random
from datetime import datetime, timedelta
from pathlib import Path

from faker import Faker

fake = Faker()
Faker.seed(42)
random.seed(42)

def generate_clients(n_clients: int, output_path:str) -> list[int]:
    """
    Generate fake client data

    Args:
        n_clients (int): Number of client to generate
        output_path (str): Path to save the client csv file

    Returns:
        list[int]: List of clients IDs
    """

    countries = ["France", "Germany",  "Spain", "Italy", "Belgium", "Netherland", "Switzerland", "UK", "Canada"]

    clients = []
    client_ids = []

    for i in range(1, n_clients + 1):
        date_inscription = fake.date_between(start_date = "-3y", end_date="-1m")
        clients.append({
            "id_client": i,
            "nom": fake.name(),
            "email": fake.email(),
            "date_inscription": date_inscription.strftime("%Y-%m-%d"),
            "pays": random.choice(countries)
        })

        client_ids.append(i)

    Path(output_path).parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id_client",  "nom", "email", "date_inscription", "pays"])
        writer.writeheader()
        writer.writerows(clients)

    print(f"Generated Clients: {n_clients} in file {output_path}")
    return client_ids

def generate_achats(client_ids: list[int], avg_purchases_per_client: int, output_path: str) -> None:
    """
    Generate fake purchase data

    Args:
        client_ids: List of client IDs with
        id_achat, id_client, data_achat, montant, produit
        avg_purchases_per_client: Average number of purchases per client
        output_path: Path to save achats.csv,
    """
    products = ["Laptop", "Phone", "Tablet", "Headphones", "Monitor",  "Keyboard", "Mouse", "Webcam", "Speaker", "Charger"]
    
    achats = []
    id_achat = 1
    
    # Prix moyens pour chaque produit (en euros)
    product_prices = {
        "Laptop": (800, 2500),
        "Phone": (200, 1200),
        "Tablet": (150, 800),
        "Headphones": (50, 400),
        "Monitor": (100, 800),
        "Keyboard": (30, 200),
        "Mouse": (10, 150),
        "Webcam": (40, 300),
        "Speaker": (50, 500),
        "Charger": (15, 100)
    }
    
    for client_id in client_ids:
        # Générer un nombre variable d'achats par client (autour de la moyenne)
        n_achats = max(1, int(random.gauss(avg_purchases_per_client, avg_purchases_per_client * 0.5)))
        
        for _ in range(n_achats):
            produit = random.choice(products)
            # Générer un montant aléatoire dans la plage de prix du produit
            min_price, max_price = product_prices[produit]
            montant = round(random.uniform(min_price, max_price), 2)
            
            # Générer une date d'achat aléatoire (depuis l'inscription jusqu'à aujourd'hui)
            date_achat = fake.date_between(start_date="-3y", end_date="today")
            
            achats.append({
                "id_achat": id_achat,
                "id_client": client_id,
                "date_achat": date_achat.strftime("%Y-%m-%d"),
                "montant": montant,
                "produit": produit
            })
            
            id_achat += 1
    
    Path(output_path).parent.mkdir(parents=True, exist_ok=True)
    
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id_achat", "id_client", "date_achat", "montant", "produit"])
        writer.writeheader()
        writer.writerows(achats)
    
    print(f"Generated {len(achats)} purchases in file {output_path}")


if __name__ == "__main__":
    output_dir = Path(__file__).parent.parent / "data" / "sources"

    client_ids = generate_clients(
        n_clients=1500,
        output_path=str(output_dir / "clients.csv")
    )
    
    generate_achats(
        client_ids=client_ids,
        avg_purchases_per_client=5,
        output_path=str(output_dir / "achats.csv")
    )