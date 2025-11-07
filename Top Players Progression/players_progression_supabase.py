import requests
import pandas as pd
import time
from datetime import datetime
from supabase import create_client, Client
from supabase.lib.client_options import ClientOptions
from requests.exceptions import SSLError
import urllib3
import os

# Désactiver le warning pour les requêtes non vérifiées
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ------------------------
# ⚙️ CONFIGURATION SUPABASE
# ------------------------
opts = ClientOptions(
    schema="api",
    postgrest_client_timeout=60
)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, opts)

# ------------------------
# 📅 PARAMÈTRES
# ------------------------
limit = 1500
pause_duration = 5
today_str = datetime.now().strftime("%Y-%m-%d")

# ------------------------
# 📥 FONCTIONS API
# ------------------------
def get_players_batch(before_player_id=None, limit=1500):
    base_url = "https://z519wdyajg.execute-api.us-east-1.amazonaws.com/prod/players"
    params = {"limit": limit, "excludingMflOwned": "true"}
    if before_player_id:
        params["beforePlayerId"] = before_player_id
    full_url = requests.Request("GET", base_url, params=params).prepare().url
    print(f"URL appelée : {full_url}")
    try:
        response = requests.get(base_url, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        print(f"✅ {len(data)} joueurs reçus dans ce lot.")
        return data
    except Exception as e:
        print(f"❌ Erreur API : {e}")
        return None

def extract_players_from_batch(json_data):
    if not json_data or not isinstance(json_data, list):
        return []
    players_list = []
    for player in json_data:
        metadata = player.get("metadata", {})
        player_info = {
            "playerid": player.get("id"),
            "nom": f"{metadata.get('firstName', '')} {metadata.get('lastName', '')}".strip(),
            "age": metadata.get("age"),
            "overall": metadata.get("overall"),
            "positions": ", ".join(metadata.get("positions", [])),
            "pace": metadata.get("pace"),
            "shooting": metadata.get("shooting"),
            "passing": metadata.get("passing"),
            "dribbling": metadata.get("dribbling"),
            "defense": metadata.get("defense"),
            "physical": metadata.get("physical"),
            "goalkeeping": metadata.get("goalkeeping"),
            "processed_date": today_str
        }
        players_list.append(player_info)
    return players_list

# ------------------------
# 🚀 TRAITEMENT DES LOTS
# ------------------------
all_players = []
batch_count = 0
before_player_id = None
start_time = time.time()
print("🚀 Démarrage de la récupération des joueurs...")

while True:
    batch_count += 1
    print(f"\n📦 Lot #{batch_count} ...")
    data = get_players_batch(before_player_id=before_player_id, limit=limit)
    if not data:
        print("✅ Aucun joueur supplémentaire trouvé, arrêt du script.")
        break

    players_batch = extract_players_from_batch(data)
    all_players.extend(players_batch)

    # Récupérer uniquement les données existantes pour les joueurs du lot actuel
    player_ids = [player["playerid"] for player in players_batch]
    try:
        old_players_batch = supabase.table("players").select("*").in_("playerid", player_ids).execute().data
        df_old_batch = pd.DataFrame(old_players_batch) if old_players_batch else pd.DataFrame()
        print(f"📊 {len(df_old_batch)} joueurs existants chargés depuis Supabase pour ce lot.")
    except Exception as e:
        print("⚠️ Erreur lors du chargement des joueurs existants pour ce lot :", e)
        df_old_batch = pd.DataFrame()

    # Traitement des données de progression pour ce lot
    if not df_old_batch.empty:
        df_merged = pd.merge(pd.DataFrame(players_batch), df_old_batch, on="playerid", suffixes=("_new", "_old"), how="left")
        attrs = ["overall", "pace", "shooting", "passing", "dribbling", "defense", "physical", "goalkeeping"]
        changed_rows = []
        new_players = []

        for _, row in df_merged.iterrows():
            is_new_player = pd.isna(row.get("nom_old"))

            if is_new_player:
                new_players.append({
                    "playerid": row["playerid"],
                    "nom": row["nom_new"],
                    "age": row["age_new"],
                    "overall": row["overall_new"],
                    "positions": row["positions_new"],
                    "pace": row["pace_new"],
                    "shooting": row["shooting_new"],
                    "passing": row["passing_new"],
                    "dribbling": row["dribbling_new"],
                    "defense": row["defense_new"],
                    "physical": row["physical_new"],
                    "goalkeeping": row["goalkeeping_new"],
                    "processed_date": today_str
                })
            else:
                differences = {}
                for attr in attrs:
                    if row.get(f"{attr}_new") != row.get(f"{attr}_old"):
                        try:
                            # Convertir les valeurs en entiers
                            new_value = int(float(row.get(f"{attr}_new") or 0))
                            old_value = int(float(row.get(f"{attr}_old") or 0))
                            differences[attr] = new_value - old_value
                        except (TypeError, ValueError):
                            continue
                if differences:
                    entry = {"playerid": row["playerid"], "processed_date": today_str, **differences}
                    changed_rows.append(entry)

        # Insertion des nouveaux joueurs
        if new_players:
            try:
                supabase.table("players").insert(new_players).execute()
                print(f"✅ {len(new_players)} nouveaux joueurs insérés dans players.")
            except Exception as e:
                print(f"⚠️ Erreur insertion nouveaux joueurs lot {batch_count} :", e)

        # Mise à jour des joueurs existants
        for _, row in df_merged.iterrows():
            if not pd.isna(row.get("nom_old")):
                player_id = row["playerid"]
                player_data = {attr: int(float(row[f"{attr}_new"] or 0)) for attr in attrs}
                try:
                    supabase.table("players").update(player_data).eq("playerid", player_id).execute()
                except Exception as e:
                    print(f"⚠️ Erreur mise à jour joueur {player_id} :", e)

        # Insertion des évolutions
        if changed_rows:
            try:
                supabase.table("players_progression").insert(changed_rows).execute()
                print(f"✅ {len(changed_rows)} évolutions insérées dans players_progression.")
            except Exception as e:
                print(f"⚠️ Erreur insertion évolutions lot {batch_count} dans players_progression :", e)
        else:
            print(f"📉 Aucune évolution détectée pour ce lot de joueurs.")
    else:
        # Si df_old_batch est vide, insérer tous les joueurs comme nouveaux
        try:
            supabase.table("players").insert(players_batch).execute()
            print(f"✅ {len(players_batch)} joueurs insérés dans players.")
        except Exception as e:
            print(f"⚠️ Erreur insertion joueurs lot {batch_count} :", e)

    before_player_id = players_batch[-1]["playerid"]
    print(f"➡️ Prochain beforePlayerId = {before_player_id}")
    print(f"⏸️ Pause de {pause_duration}s avant le prochain lot...")
    time.sleep(pause_duration)

# ------------------------
# ⏱️ TEMPS TOTAL
# ------------------------
elapsed_total = time.time() - start_time
h, m, s = int(elapsed_total // 3600), int((elapsed_total % 3600) // 60), int(elapsed_total % 60)
print(f"\n🏁 Traitement terminé en {h:02d}:{m:02d}:{s:02d} — Total {len(all_players)} joueurs.")
