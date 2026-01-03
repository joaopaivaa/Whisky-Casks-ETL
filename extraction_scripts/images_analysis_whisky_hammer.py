from openai import OpenAI
import pandas as pd
from tqdm import tqdm
import os
from dotenv import load_dotenv
import re
import json

load_dotenv(dotenv_path=".env")

openai_api_key = os.getenv("OPENAI_API_KEY")

client = OpenAI(
    api_key=openai_api_key
)

prompt = """

You are an assistant that extracts structured data from images.

Analyze the image and return ONLY valid JSON with the following fields:
- distillery (string or null)
- filling_year (string or null)
- warehouse (string or null)
- regauged_date (string or null)
- bulk_litres (string or null)
- strength (string or null)
- cask_type (string or null)

If a field is not visible or cannot be inferred, return null.
Do not add extra fields.
Do not include explanations.

"""

def analyze_image_url(image_url):
    response = client.chat.completions.create(
        model="gpt-4.1",  # modelo multimodal
        messages=[
            {
                "role": "user",
                "content": [
                    {"type": "text", "text": prompt},
                    {
                        "type": "image_url",
                        "image_url": {
                            "url": image_url
                        }
                    }
                ]
            }
        ],
        temperature=0
    )

    return response.choices[0].message.content

def parse_gpt_json(output: str) -> pd.Series:
    # Remove ```json e ```
    cleaned = re.sub(r"```json|```", "", output).strip()

    # Parse JSON
    data = json.loads(cleaned)

    return pd.Series(data)

df = pd.read_csv("bronze\\Whisky Hammer - Casks Database.csv", sep=";")

results = []

for url in tqdm(df["image"]):
    try:
        output = analyze_image_url(url)
        row = parse_gpt_json(output)
        row["image"] = url
        results.append(row)
    except Exception as e:
        print(f"Erro na imagem {url}: {e}")

df_final = pd.DataFrame(results)

df_final.to_csv("bronze\\Whisky Hammer - Images Analysis.csv", sep=";", index=False)