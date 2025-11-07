# supabase_utils.py
from supabase import create_client, Client
from config import SUPABASE_URL, SUPABASE_KEY
from supabase.lib.client_options import ClientOptions
from requests.exceptions import SSLError
import urllib3

# Désactiver le warning pour les requêtes non vérifiées
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

opts = ClientOptions().replace(schema="api")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY, opts)

def get_top_n(n: int, attribute: str):
    """Récupère le Top 10 des progressions pour un attribut donné."""
    try:
        # Récupère la date la plus récente
        latest_date_result = supabase.rpc('get_latest_date').execute()
        max_date = latest_date_result.data

        # Récupère la date précédente
        previous_date_result = supabase.rpc('get_previous_date', {'max_date': max_date}).execute()
        prev_date = previous_date_result.data

        # Exécute la requête pour récupérer le Top 10
        result = supabase.rpc('get_top_n_players', {
            'n_value': n, 
            'attribute': attribute,
            'max_date': max_date
        }).execute()

        return result.data
    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return []

def get_top_n_gk(n: int, attribute: str):
    """Récupère le Top 10 des progressions pour un attribut donné."""
    try:
        # Récupère la date la plus récente
        latest_date_result = supabase.rpc('get_latest_date').execute()
        max_date = latest_date_result.data

        # Récupère la date précédente
        previous_date_result = supabase.rpc('get_previous_date', {'max_date': max_date}).execute()
        prev_date = previous_date_result.data

        # Exécute la requête pour récupérer le Top 10
        result = supabase.rpc('get_top_n_gk', {
            'n_value': n, 
            'max_date': max_date
        }).execute()

        return result.data
    except Exception as e:
        print(f"Erreur lors de la récupération des données : {e}")
        return []
