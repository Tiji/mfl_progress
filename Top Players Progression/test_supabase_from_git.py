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
    schema="api"
)

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_KEY")

# Désactiver la vérification SSL si nécessaire
opts.postgrest_client_extra_headers = {"Prefer": "return=representation"}
opts.postgrest_client_timeout = 60

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, opts)

old_players_batch = supabase.table("players").select("*").eq("playerid", 153041).execute().data
df_old_batch = pd.DataFrame(old_players_batch) if old_players_batch else pd.DataFrame()
print(f"📊 {len(df_old_batch)} joueurs existants chargés depuis Supabase pour ce lot.")