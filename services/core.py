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


async def normalize_product_name(product_name, prompt_template):
    if pd.isna(product_name) or not str(product_name).strip():
        return {} # Возвращаем пустой словарь для пустых ячеек

    prompt = prompt_template.format(product_name=product_name)
    try:
        response = client.chat.completions.create(
            model=MODEL_NAME,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.0,
            response_format={"type": "json_object"}
        )
        json_response = response.choices[0].message.content
        return json.loads(json_response)
    except Exception as e:
        print(f"!!! Ошибка при обработке '{product_name}': {e}")
        return {"error": str(e)}


async def make_request_normalize_excel_driver(excel_file, column_name, prompt_str):
    if not os.getenv("OPENAI_API_KEY"):
        print("Критическая ошибка: Переменная окружения OPENAI_API_KEY не установлена.")
        exit()

    INPUT_FILE = excel_file
    OUTPUT_FILE = f"{excel_file}_processed.xlsx" 
    SOURCE_COLUMN = column_name
    SAVE_INTERVAL = 100 

    try:
        df = pd.read_excel(INPUT_FILE)
        print(f"Файл '{INPUT_FILE}' успешно загружен. Всего {len(df)} строк.")
    except FileNotFoundError:
        print(f"ОШИБКА: Исходный файл '{INPUT_FILE}' не найден.")
        exit()

    if 'normalized_data' not in df.columns:
        df['normalized_data'] = None

    rows_to_process = df[df['normalized_data'].isnull()]
    print(f"{len(rows_to_process)} строк осталось для обработки.")

    tqdm.pandas(desc="Обработка товаров")

    for index, row in tqdm(rows_to_process.iterrows(), total=len(rows_to_process)):
        product_name = row[SOURCE_COLUMN]

        normalized_result = await normalize_product_name(product_name, prompt_str)

        df.loc[index, 'normalized_data'] = json.dumps(normalized_result)

        if (index + 1) % SAVE_INTERVAL == 0:
            temp_normalized_df = pd.json_normalize(
                df['normalized_data'].dropna().apply(json.loads)
            )
            final_df = df.drop(columns=['normalized_data']).join(temp_normalized_df)
            final_df.to_excel(OUTPUT_FILE, index=False)



    # --- ФИНАЛЬНОЕ СОХРАНЕНИЕ ---
    print("\nОбработка завершена. Финальное сохранение...")
    final_normalized_df = pd.json_normalize(
        df['normalized_data'].dropna().apply(json.loads)
    )
    final_result_df = df.drop(columns=['normalized_data']).join(final_normalized_df)
    
    final_result_df.to_excel(OUTPUT_FILE, index=False)
    print(f"Результат успешно сохранен в файл: '{OUTPUT_FILE}'")

    return OUTPUT_FILE