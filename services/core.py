from openai import OpenAI
from dotenv import load_dotenv
import json
import pandas as pd
from tqdm import tqdm
import asyncio
import os
load_dotenv()

client = OpenAI()
MODEL_NAME = "gpt-4o-mini"
BATCH_SIZE = 20
SAVE_INTERVAL = 100

async def normalize_product_name(product_list, prompt_template):
    """Processes a batch of product names with the user-supplied prompt."""
    # Build the batched prompt with the user format
    formatted = "\n".join([f"{i+1}. {item}" for i, item in enumerate(product_list)])
    prompt = prompt_template.format(product_list=formatted)

    try:
        response = await client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            response_format={"type": "json_object"},
        )
        result = response.choices[0].message.content
        return json.loads(result)
    except Exception as e:
        print(f"⚠️ Ошибка при обработке батча: {e}")
        return [{"error": str(e)} for _ in product_list]


async def make_request_normalize_excel_driver(excel_file, column_name, prompt_str):
    if not os.getenv("OPENAI_API_KEY"):
        print("Критическая ошибка: Переменная окружения OPENAI_API_KEY не установлена.")
        exit()

    OUTPUT_FILE = f"{excel_file}_processed.xlsx"

    try:
        df = pd.read_excel(excel_file)
        print(f"Файл '{excel_file}' успешно загружен. Всего {len(df)} строк.")
    except FileNotFoundError:
        print(f"ОШИБКА: Исходный файл '{excel_file}' не найден.")
        exit()

    if 'normalized_data' not in df.columns:
        df['normalized_data'] = None

    rows_to_process = df[df['normalized_data'].isnull()]
    product_names = rows_to_process[column_name].tolist()
    row_indices = rows_to_process.index.tolist()

    print(f"{len(product_names)} строк осталось для обработки.")

    for i in tqdm(range(0, len(product_names), BATCH_SIZE), desc="Обработка батчей"):
        batch = product_names[i:i + BATCH_SIZE]
        normalized_batch = await normalize_product_name(batch, prompt_str)

        for j, result in enumerate(normalized_batch):
            df.loc[row_indices[i + j], 'normalized_data'] = json.dumps(result)

        if (i + BATCH_SIZE) % SAVE_INTERVAL == 0 or (i + BATCH_SIZE) >= len(product_names):
            temp_normalized_df = pd.json_normalize(
                df['normalized_data'].dropna().apply(json.loads)
            )
            final_df = df.drop(columns=['normalized_data']).join(temp_normalized_df)
            final_df.to_excel(OUTPUT_FILE, index=False)

    print(f"\n✅ Обработка завершена. Результат сохранен в файл: '{OUTPUT_FILE}'")
    return OUTPUT_FILE