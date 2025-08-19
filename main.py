from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import argparse
import os
import shutil
import xml.etree.ElementTree as ET
import visualize 

# Функция для загрузки параметров из конфигурационного файла
def load_config(config_path):

    params = {
        'candle.width': 300000,
        'candle.date.from': '19000101',
        'candle.date.to': '20200101',
        'candle.time.from': '1000',
        'candle.time.to': '1800'
    }

    if not config_path or not os.path.exists(config_path):
        return params
    try:
        tree = ET.parse(config_path)
        root = tree.getroot()
        
        for prop in root.findall('property'):
            name = prop.find('name').text.strip()
            value = prop.find('value').text.strip()
            
            if name == 'candle.width':
                params[name] = int(value)
            elif name in params:
                params[name] = value
                
    except (ET.ParseError, AttributeError) as e:
        print(f"Ошибка чтения конфига: {e}. Используются значения по умолчанию.")
    
    return params
# Функция для получения DataFrame из CSV-файла
def get_df_from_csv(spark, path, params):
    if not os.path.exists(path):
        raise FileNotFoundError(f"Файл {path} не найден")
    
    df = spark.read.csv(path, header=True, inferSchema=False, sep=",")
    df = df.withColumn("PRICE_DEAL", F.col("PRICE_DEAL").cast("double"))

    if df.isEmpty():
        raise ValueError(f"Файл {path} пуст")

    # Выбираем нужные колонки
    df = df.select(
        F.col("#SYMBOL").alias("SYMBOL"),
        "MOMENT",
        "PRICE_DEAL",
        "ID_DEAL"
    )

    # Парсим дату и время
    df = df.withColumn("date_str", F.substring("MOMENT", 1, 8))
    df = df.withColumn("time_str", F.substring("MOMENT", 9, 9))

    # Преобразуем время в миллисекунды
    df = df.withColumn("hh", F.substring("time_str", 1, 2).cast("int"))
    df = df.withColumn("mm", F.substring("time_str", 3, 2).cast("int"))
    df = df.withColumn("ss", F.substring("time_str", 5, 2).cast("int"))
    df = df.withColumn("fff", F.substring("time_str", 7, 3).cast("int"))
    df = df.withColumn("total_ms", (F.col("hh")*3600000 + F.col("mm")*60000 + F.col("ss")*1000 + F.col("fff")))

    # Конвертируем параметры времени в миллисекунды
    time_from_ms = int(params['candle.time.from'][:2])*3600000 + int(params['candle.time.from'][2:])*60000
    time_to_ms = int(params['candle.time.to'][:2])*3600000 + int(params['candle.time.to'][2:])*60000

    # Фильтрация
    df = df.filter(
        (F.col("date_str") >= params['candle.date.from']) &
        (F.col("date_str") < params['candle.date.to']) &
        (F.col("total_ms") >= time_from_ms) &
        (F.col("total_ms") < time_to_ms)
    )

    return df

# Функция для расчета свечей
def make_candles_df(df, params):
    candle_width = params['candle.width']
    df = df.withColumn("start_candle_ms", (F.floor(F.col("total_ms") / candle_width) * candle_width)) 

    # Агрегируем HIGH/LOW/OPEN/CLOSE
    w = Window.partitionBy("SYMBOL", "date_str", "start_candle_ms").orderBy("total_ms", "ID_DEAL")
    w = w.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    df = df.withColumn("OPEN", F.first("PRICE_DEAL").over(w))
    df = df.withColumn("CLOSE", F.last("PRICE_DEAL").over(w))
    df = df.withColumn("HIGH", F.max("PRICE_DEAL").over(w))
    df = df.withColumn("LOW", F.min("PRICE_DEAL").over(w))

    # Убираем дубликаты по ключу свечи, оставляем только одну строку
    result_df = df.dropDuplicates(["SYMBOL", "date_str", "start_candle_ms"])

    # Конвертируем start_candle_ms в строку времени
    result_df = result_df.withColumn("hours", (F.col("start_candle_ms") / 3600000).cast("int"))
    result_df = result_df.withColumn("start_candle_ms", F.col("start_candle_ms") % 3600000)
    result_df = result_df.withColumn("minutes", (F.col("start_candle_ms") / 60000).cast("int"))
    result_df = result_df.withColumn("start_candle_ms", F.col("start_candle_ms") % 60000)
    result_df = result_df.withColumn("seconds", (F.col("start_candle_ms") / 1000).cast("int"))
    result_df = result_df.withColumn("millis", F.col("start_candle_ms") % 1000)

    # Форматируем компоненты
    result_df = result_df.withColumn("time_str", 
        F.concat(
            F.lpad(F.col("hours"), 2, "0"),
            F.lpad(F.col("minutes"), 2, "0"),
            F.lpad(F.col("seconds"), 2, "0"),
            F.lpad(F.col("millis"), 3, "0")
        )
    )

    # Создаём MOMENT и округляем цены
    result_df = result_df.withColumn("MOMENT", F.concat("date_str", "time_str"))
    result_df = result_df.withColumn("OPEN", F.round("OPEN", 1))
    result_df = result_df.withColumn("HIGH", F.round("HIGH", 1))
    result_df = result_df.withColumn("LOW", F.round("LOW", 1))
    result_df = result_df.withColumn("CLOSE", F.round("CLOSE", 1))

    # Выбираем финальные колонки
    final_df = result_df.select("SYMBOL", "MOMENT", "OPEN", "HIGH", "LOW", "CLOSE").orderBy("SYMBOL", "MOMENT")
    return final_df

def save_results(final_df, output_dir):

    os.makedirs(output_dir, exist_ok=True)

    # Сохраняем все символы
    temp_path = os.path.join(output_dir, "_temp_all")

    final_df = final_df.withColumn("partition_key", F.col("SYMBOL")).select("partition_key", "SYMBOL", "MOMENT", "OPEN", "HIGH", "LOW", "CLOSE")

    final_df.repartition("partition_key").write.partitionBy("partition_key").csv(
        temp_path, mode="overwrite", header=False
    )

    # Проходим по сгенерированным папкам SYMBOL=XXX
    for folder in os.listdir(temp_path):
        folder_path = os.path.join(temp_path, folder)
        if os.path.isdir(folder_path) and folder.startswith("partition_key="):
            symbol = folder.split("=", 1)[1]
            # Ищем CSV-файл в папке
            for file in os.listdir(folder_path):
                if file.startswith("part-"):
                    os.rename(
                        os.path.join(folder_path, file),
                        os.path.join(output_dir, f"{symbol}.csv")
                    )

    # Удаляем временную директорию
    shutil.rmtree(temp_path)

    print(f"Результаты сохранены в: {output_dir}")
def main():
    # Парсинг аргументов командной строки
    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="Путь к входному CSV-файлу")
    parser.add_argument("output", help="Директория для результатов")
    parser.add_argument("--config", help="Путь к config.xml (опционально)")
    parser.add_argument("--visual", help="При добавлении этого параметра будут нарисованы свечевые графики", action="store_true")
    args = parser.parse_args()

    # Параметры по умолчанию
    params = load_config(args.config)
    # Создаём Spark-сессию
    spark = SparkSession.builder.appName("CandlesBuilder").getOrCreate()
    
    try:
        print("Обработка начата...")
        df = get_df_from_csv(spark, args.input, params)
        candles_df = make_candles_df(df, params)
        candles_df.persist()
        candles_df.count()
        print("Обработка завершена. Сохранение результатов...")
        save_results(candles_df, args.output)
        if(args.visual):
            print("Генерация графиков...")
            visualize.plot_all_candles(params['candle.width'], args.output, os.path.join(args.output, 'plots'))
    except Exception as e:
        print(f"Ошибка: {str(e)}")
    finally:
        spark.stop()

if __name__ == "__main__":
    main()

