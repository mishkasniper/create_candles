import os
import pandas as pd
import matplotlib.pyplot as plt
from matplotlib.dates import DateFormatter, WeekdayLocator, HourLocator
from mplfinance.original_flavor import candlestick_ohlc
import matplotlib.dates as mdates

def plot_candles_from_csv(width, csv_path, output_dir, 
                         figsize=(16, 8), dpi=100, 
                         color_up='#77d879', color_down='#db3f3f'):
                         
    # Загрузка и подготовка данных
    column_names = ['MOMENT', 'OPEN', 'HIGH', 'LOW', 'CLOSE']
    df = pd.read_csv(csv_path, header=None, names=column_names)
    
    # Проверка данных
    if df.empty:
        print(f"Файл {csv_path} пуст, график не будет построен")
        return
    
    # Преобразование времени
    df['datetime'] = pd.to_datetime(df['MOMENT'], format='%Y%m%d%H%M%S%f')
    df['date_num'] = mdates.date2num(df['datetime'])
    
    # Сортировка по времени
    df = df.sort_values('datetime')
    
    # Подготовка данных для свечей
    ohlc = df[['date_num', 'OPEN', 'HIGH', 'LOW', 'CLOSE']].values
    
    # Создание графика
    fig, ax = plt.subplots(figsize=figsize, dpi=dpi)
    
    # Настройка формата даты
    ax.xaxis.set_major_locator(WeekdayLocator(byweekday=mdates.MO))
    ax.xaxis.set_minor_locator(HourLocator(interval=4))
    
    # Построение свечей
    candlestick_ohlc(
        ax, ohlc, 
        width=width/86400000, 
        colorup=color_up, 
        colordown=color_down
    )
    
    # Настройки графика
    symbol = os.path.basename(csv_path).split('.')[0]
    title = f'Свечной график {symbol}\nПериод: {df.datetime.min().date()} - {df.datetime.max().date()}'
    ax.set_title(title, fontsize=14, pad=20)
    ax.set_xlabel('Дата и время', fontsize=12)
    ax.set_ylabel('Цена', fontsize=12)
    ax.grid(True, which='both', linestyle='--', linewidth=0.5)
    
    # Автоматическое размещение подписей
    fig.autofmt_xdate()
    plt.tight_layout()
    
    # Сохранение или отображение
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)
        output_path = os.path.join(output_dir, f'{symbol}_candles.png')
        plt.savefig(output_path, bbox_inches='tight')
        plt.close()
        print(f'График сохранен: {output_path}')
    else:
        plt.show()

def plot_all_candles(width, csv_dir, output_dir, **kwargs):
    """Строит графики для всех CSV-файлов с настройками"""
    if not os.path.exists(csv_dir):
        raise FileNotFoundError(f"Директория {csv_dir} не найдена")
    
    for filename in sorted(os.listdir(csv_dir)):
        if filename.endswith('.csv'):
            csv_path = os.path.join(csv_dir, filename)
            try:
                plot_candles_from_csv(width, csv_path, output_dir, **kwargs)
            except Exception as e:
                print(f"Ошибка при обработке {filename}: {str(e)}")