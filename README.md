Требования к окружению
Версии ПО:

    Python 3.10+

    PySpark 3.5.0



Основные библиотеки и их версии:

pyspark==3.5.0
pandas==2.1.0
matplotlib==3.7.2
xmltodict==0.13.0

Установка зависимостей

Рекомендуется использовать виртуальное окружение. Установите зависимости командой:
```bash

pip install pyspark==3.5.0 pandas==2.1.0 matplotlib==3.7.2 xmltodict==0.13.0

```
или:
```bash

pip install -r requirements.txt

```

Запуск программы

Основной скрипт:
```bash

python main.py <input_file> <output_dir> [--config <config_file>]
```
Где:

    <input_file> - путь к входному CSV-файлу с данными сделок

    <output_dir> - директория для сохранения результатов

    --config (опционально) - путь к XML-файлу конфигурации



Формат входных данных

Входной файл должен быть в CSV-формате со следующими колонками:

#SYMBOL,SYSTEM,MOMENT,ID_DEAL,PRICE_DEAL,VOLUME,OPEN_POS,DIRECTION

Пример строки:

SVH1,F,20110111100000080,255223067,30.46000,1,8714,S

Формат выходных данных

Для каждого финансового инструмента создается отдельный CSV-файл в формате:

SYMBOL,MOMENT,OPEN,HIGH,LOW,CLOSE

Пример:

GDH1,20110111100000000,1407.0,1407.0,1379.0,1379.3

Конфигурационный файл

Программа поддерживает настройку параметров через XML-файл.

Пример config.xml:
```

<configuration>
    <property>
        <name>candle.width</name>
        <value>300000</value>  <!-- ширина свечи в миллисекундах -->
    </property>
    <property>
        <name>candle.date.from</name>
        <value>20110101</value>  <!-- начальная дата -->
    </property>
    <property>
        <name>candle.date.to</name>
        <value>20200101</value>  <!-- конечная дата -->
    </property>
    <property>
        <name>candle.time.from</name>
        <value>1000</value>  <!-- время начала (ЧЧММ) -->
    </property>
    <property>
        <name>candle.time.to</name>
        <value>1800</value>  <!-- время окончания (ЧЧММ) -->
    </property>
</configuration>
```
