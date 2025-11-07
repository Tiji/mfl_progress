# main.py
from discord import Intents, Client, Message, Embed
from config import DISCORD_TOKEN, YOUR_DISCORD_ID
from supabase_utils import get_top_n, get_top_n_gk 

# Initialisation du client Discord
intents = Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True

client = Client(intents=intents)

@client.event
async def on_ready():
    print(f"Bot connecté en tant que {client.user}")

@client.event
async def on_message(message: Message):
    # Ignore les messages du bot lui-même
    if message.author == client.user:
        return
    # # Seuls tes messages déclenchent le bot
    # if message.author.id != YOUR_DISCORD_ID:
        # return

    # Vérifie si le message commence par "!top"
    if message.content.startswith("!top"):
        parts = message.content.split()
        if len(parts) < 3:
            await message.channel.send("Format incorrect. Utilisez `!top <nombre> <attribut>` (ex: `!top 10 ovr`).")
            return

        try:
            n = int(parts[1])
            attribute = parts[2].lower()
            title = parts[2].upper()

            if attribute == "gk":
                await send_top_n(message, n, "goalkeeping", "Goalkeeping", get_top_n_gk)
            else:
                await send_top_n(message, n, attribute, title.capitalize())
        except ValueError:
            await message.channel.send("Le nombre doit être un entier valide.")
        except Exception as e:
            await message.channel.send(f"⚠️ Erreur lors du traitement de la commande : {e}")

async def send_top_n(message: Message, n: int, attribute: str, title: str, get_top_function=None):
    get_top_function = get_top_function or get_top_n
    try:
        top_n = get_top_function(n, attribute)
        if not top_n:
            await message.channel.send(f"Aucune donnée disponible pour le Top {n} {title}.")
            return
        embed = Embed(title=f"🏆 Top {n} {title}", color=0x00ff00)

        # Crée une seule chaîne de caractères pour tous les joueurs
        players_text = ""
        for idx, row in enumerate(top_n, start=1):
            player_name = row['player_name']
            player_id = row['playerid']
            progress = row['progress']
            current_value = row[f'current_value']
            previous_value = row[f'previous_value']

            emoji = "🥇" if idx == 1 else "🥈" if idx == 2 else "🥉" if idx == 3 else f"{idx}."

            # Ajoute le lien cliquable et les statistiques sur une seule ligne
            player_link = f"[**{player_name}**](https://app.playmfl.com/fr/players/{player_id})"
            player_line = f"{emoji} {player_link} — +{progress} : {previous_value} > {current_value}\n"
            players_text += player_line

        # Ajoute tous les joueurs dans un seul champ
        embed.add_field(name="\u200b", value=players_text, inline=False)

        await message.channel.send(embed=embed)
        
    except Exception as e:
        await message.channel.send(f"⚠️ Erreur lors de la récupération du Top {n} {title} : {e}")

# Lance le bot
client.run(DISCORD_TOKEN)