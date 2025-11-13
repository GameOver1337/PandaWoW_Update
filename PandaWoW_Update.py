#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для скачивания файлов и обновлений с поддержкой SOCKS5 прокси
Основан на логике PandaWoW Launcher
"""

import os
import sys
import json
import hashlib
import base64
import requests
from pathlib import Path
from typing import Optional, Dict, List, Tuple
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm


class LogHandler:
    """Обработчик логов для вывода в консоль"""
    
    def __init__(self):
        self.logs = []
    
    def log(self, message: str, level: str = "INFO"):
        """Добавить сообщение в лог"""
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_entry = f"[{timestamp}] [{level}] {message}"
        self.logs.append(log_entry)
        print(log_entry)


class ProxyDownloader:
    """Класс для скачивания файлов с поддержкой SOCKS4/SOCKS5 прокси"""
    
    PROXY_LIST_SOCKS5_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks5/data.json"
    PROXY_LIST_SOCKS4_URL = "https://cdn.jsdelivr.net/gh/proxifly/free-proxy-list@main/proxies/protocols/socks4/data.json"
    
    def __init__(self, proxy_url: Optional[str] = None, log_handler: Optional[LogHandler] = None):
        """
        Инициализация загрузчика
        
        Args:
            proxy_url: URL прокси в формате socks5://user:pass@host:port или socks5://host:port
            log_handler: Обработчик логов
        """
        self.proxy_url = proxy_url
        self.log_handler = log_handler or LogHandler()
        self.session = self._create_session()
        
        # API настройки (из UrlManager.cs)
        self.api_host = "https://api.pandawow-tools.com/Launcher"
        self.api_key = "Jra4H5d63meGY7xr3T8XFN2t"
        self.launcher_url = None  # Будет получена из API
        
    def _create_session(self) -> requests.Session:
        """Создание сессии с настройками прокси"""
        session = requests.Session()
        session.headers.update({'User-Agent': 'PandaWoW-Downloader/1.0'})
        
        if self.proxy_url:
            proxies = {
                'http': self.proxy_url,
                'https': self.proxy_url
            }
            session.proxies.update(proxies)
            self.log_handler.log(f"Используется прокси: {self.proxy_url}", "INFO")
        
        return session
    
    def fetch_proxy_list(self) -> List[Dict]:
        """Получение списка SOCKS4 и SOCKS5 прокси"""
        self.log_handler.log("Загрузка списков прокси...", "INFO")
        all_proxies = []
        
        # Загружаем SOCKS5
        try:
            response = requests.get(self.PROXY_LIST_SOCKS5_URL, timeout=10)
            response.raise_for_status()
            socks5_proxies = response.json()
            self.log_handler.log(f"Загружено {len(socks5_proxies)} SOCKS5 прокси", "OK")
            all_proxies.extend(socks5_proxies)
        except Exception as e:
            self.log_handler.log(f"Ошибка загрузки SOCKS5 прокси: {e}", "ERROR")
        
        # Загружаем SOCKS4
        try:
            response = requests.get(self.PROXY_LIST_SOCKS4_URL, timeout=10)
            response.raise_for_status()
            socks4_proxies = response.json()
            self.log_handler.log(f"Загружено {len(socks4_proxies)} SOCKS4 прокси", "OK")
            all_proxies.extend(socks4_proxies)
        except Exception as e:
            self.log_handler.log(f"Ошибка загрузки SOCKS4 прокси: {e}", "ERROR")
        
        if all_proxies:
            self.log_handler.log(f"Всего загружено {len(all_proxies)} прокси (SOCKS4 + SOCKS5)", "OK")
        
        return all_proxies
    
    def test_proxy(self, proxy_url: str, timeout: int = 3) -> tuple:
        """
        Проверка работоспособности прокси с измерением пинга
        
        Returns:
            (success: bool, ping_ms: float) - успешность проверки и пинг в миллисекундах
        """
        try:
            test_session = requests.Session()
            test_session.proxies = {
                'http': proxy_url,
                'https': proxy_url
            }
            test_session.headers.update({'User-Agent': 'PandaWoW-Downloader/1.0'})
            
            # Проверяем ТОЛЬКО на API сервере (убираем лишнюю проверку Google)
            try:
                start_time = datetime.now()
                response = test_session.get(f"{self.api_host}/PatchData?api_key={self.api_key}", timeout=timeout)
                ping_ms = (datetime.now() - start_time).total_seconds() * 1000
                
                if response.status_code == 200:
                    try:
                        data = response.json()
                        # Проверяем оба варианта регистра ключей
                        if 'files' in data or 'Files' in data or 'launcherUrl' in data or 'LauncherUrl' in data:
                            return (True, ping_ms)
                    except:
                        return (False, 9999)
                return (False, 9999)
            except:
                # Если API недоступен через прокси - прокси НЕ подходит
                return (False, 9999)
            
        except Exception as e:
            return (False, 9999)
    
    def auto_select_proxy(self, max_workers: int = 100) -> Optional[str]:
        """Автоматический выбор рабочего прокси с параллельной проверкой"""
        self.log_handler.log("Начинаю автоматический выбор прокси...", "INFO")
        
        proxies = self.fetch_proxy_list()
        if not proxies:
            self.log_handler.log("Список прокси пуст", "WARNING")
            return None
        
        # Фильтруем и сортируем прокси
        # Приоритет: score > 0, затем по убыванию score
        valid_proxies = [p for p in proxies if p.get('proxy') and p.get('score', 0) > 0]
        valid_proxies.sort(key=lambda x: x.get('score', 0), reverse=True)
        
        # Если нет прокси с score > 0, берем все
        if not valid_proxies:
            valid_proxies = [p for p in proxies if p.get('proxy')]
        
        # Тестируем ВСЕ прокси до нахождения рабочего
        proxies_to_test = valid_proxies
        self.log_handler.log(f"Буду тестировать все {len(proxies_to_test)} прокси до нахождения рабочего", "INFO")
        
        tested = 0
        working_proxies = []  # Список рабочих прокси с пингом
        
        # Функция для тестирования одного прокси
        def test_single_proxy(proxy_info):
            proxy_url = proxy_info.get('proxy')
            if not proxy_url:
                return None
            
            country = proxy_info.get('geolocation', {}).get('country', 'Unknown')
            score = proxy_info.get('score', 0)
            
            success, ping_ms = self.test_proxy(proxy_url)
            if success:
                return (proxy_url, country, score, ping_ms)
            return None
        
        # Параллельное тестирование ВСЕХ прокси с прогресс-баром
        self.log_handler.log("Проверяю все прокси для выбора самого быстрого...", "INFO")
        with tqdm(total=len(proxies_to_test), desc="Тестирование прокси", unit="proxy", colour='cyan') as pbar:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_proxy = {executor.submit(test_single_proxy, proxy): proxy for proxy in proxies_to_test}
                
                for future in as_completed(future_to_proxy):
                    tested += 1
                    result = future.result()
                    pbar.update(1)
                    
                    if result:
                        working_proxies.append(result)
                        proxy_url, country, score, ping_ms = result
                        pbar.set_postfix_str(f"Найдено: {len(working_proxies)}, лучший пинг: {min(p[3] for p in working_proxies):.0f}ms")
        
        if working_proxies:
            # Сортируем по пингу (самый быстрый первый)
            working_proxies.sort(key=lambda x: x[3])
            best_proxy = working_proxies[0]
            proxy_url, country, score, ping_ms = best_proxy
            
            self.log_handler.log(f"Проверено {tested} прокси, найдено {len(working_proxies)} рабочих", "OK")
            self.log_handler.log(f"✓ Выбран самый быстрый прокси: {proxy_url}", "OK")
            self.log_handler.log(f"  Страна: {country}, Пинг: {ping_ms:.0f}ms, Score: {score}", "INFO")
            
            # Показываем топ-5 лучших прокси
            if len(working_proxies) > 1:
                self.log_handler.log(f"Топ-5 лучших прокси по пингу:", "INFO")
                for i, (url, ctry, scr, png) in enumerate(working_proxies[:5], 1):
                    self.log_handler.log(f"  {i}. {ctry}: {png:.0f}ms (score: {scr})", "INFO")
            
            return proxy_url
        else:
            self.log_handler.log(f"Не удалось найти рабочий прокси из {tested} проверенных", "WARNING")
            self.log_handler.log("Попробую скачать без прокси...", "INFO")
            return None
    
    def get_patch_info(self) -> Dict:
        """
        Получение информации о патчах с сервера
        
        Returns:
            Словарь с информацией о файлах для обновления
        """
        url = f"{self.api_host}/PatchData?api_key={self.api_key}"
        self.log_handler.log(f"Получение списка файлов с API", "INFO")
        
        try:
            response = self.session.get(url, timeout=10)
            response.raise_for_status()
            
            patch_data = response.json()
            
            # Сохраняем URL лаунчера из API (проверяем оба варианта регистра)
            if 'launcherUrl' in patch_data:
                self.launcher_url = patch_data['launcherUrl']
            elif 'LauncherUrl' in patch_data:
                self.launcher_url = patch_data['LauncherUrl']
            
            # Проверяем оба варианта регистра для files
            files = patch_data.get('files', patch_data.get('Files', []))
            files_count = len(files)
            self.log_handler.log(f"Получена информация о {files_count} файлах", "OK")
            
            # Нормализуем ключи к ожидаемому формату
            if 'files' in patch_data and 'Files' not in patch_data:
                patch_data['Files'] = patch_data['files']
            if 'filesToDelete' in patch_data and 'FilesToDelete' not in patch_data:
                patch_data['FilesToDelete'] = patch_data['filesToDelete']
            
            return patch_data
        except requests.exceptions.RequestException as e:
            self.log_handler.log(f"Ошибка при получении списка патчей: {e}", "ERROR")
            raise
        except json.JSONDecodeError as e:
            self.log_handler.log(f"Ошибка парсинга JSON: {e}", "ERROR")
            self.log_handler.log(f"Ответ сервера: {response.text[:500]}", "ERROR")
            raise
    
    def calculate_md5(self, file_path: str, file_size: Optional[int] = None) -> str:
        """
        Вычисление MD5 хеша файла
        
        Args:
            file_path: Путь к файлу
            file_size: Размер для проверки (для .mpq файлов)
            
        Returns:
            MD5 хеш в hex формате
        """
        md5_hash = hashlib.md5()
        
        with open(file_path, 'rb') as f:
            if file_path.lower().endswith('.mpq') and file_size:
                # Для MPQ файлов читаем только указанное количество байт
                bytes_read = 0
                while bytes_read < file_size:
                    chunk_size = min(4096, file_size - bytes_read)
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    md5_hash.update(chunk)
                    bytes_read += len(chunk)
            else:
                # Для остальных файлов читаем полностью
                for chunk in iter(lambda: f.read(4096), b''):
                    md5_hash.update(chunk)
        
        return md5_hash.hexdigest()
    
    def check_file_needs_update(self, file_info: Dict, game_path: str) -> bool:
        """
        Проверка, нужно ли обновлять файл
        
        Args:
            file_info: Информация о файле из patch info
            game_path: Путь к папке с игрой
            
        Returns:
            True если файл нужно скачать, False если файл актуален
        """
        # Поддержка обоих вариантов регистра ключей
        file_name = file_info.get('fileName', file_info.get('FileName'))
        file_size = file_info.get('size', file_info.get('Size'))
        file_hash = file_info.get('hash', file_info.get('Hash'))
        
        file_path = os.path.join(game_path, file_name)
        
        if not os.path.exists(file_path):
            return True
        
        # Быстрая проверка по размеру
        actual_size = os.path.getsize(file_path)
        if actual_size == file_size:
            # Размер совпадает, пропускаем MD5 проверку для скорости
            return False
        
        # Размер не совпадает, проверяем MD5
        try:
            file_md5 = self.calculate_md5(file_path, file_size)
            expected_md5 = file_hash.lower()
            
            if file_md5 == expected_md5:
                return False
        except Exception as e:
            self.log_handler.log(f"Ошибка при проверке файла {file_path}: {e}", "WARNING")
        
        return True

    
    def download_file(self, url: str, destination: str, file_size: int, progress_callback=None, max_retries: int = 3) -> bool:
        """
        Скачивание файла с прогресс-баром и повторными попытками
        
        Args:
            url: URL файла для скачивания
            destination: Путь для сохранения файла
            file_size: Ожидаемый размер файла
            progress_callback: Функция для обновления прогресса (downloaded, total)
            max_retries: Максимальное количество попыток
            
        Returns:
            True если скачивание успешно, False в случае ошибки
        """
        # Создаем директорию если не существует
        os.makedirs(os.path.dirname(destination), exist_ok=True)
        
        filename = os.path.basename(destination)
        
        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    self.log_handler.log(f"Попытка {attempt}/{max_retries}: {filename}", "INFO")
                
                # Увеличенный таймаут: 10 сек на подключение, 60 сек на чтение
                response = self.session.get(url, stream=True, timeout=(10, 60))
                response.raise_for_status()
                
                # Создаем прогресс-бар с tqdm (зеленый цвет)
                with tqdm(
                    total=file_size,
                    unit='B',
                    unit_scale=True,
                    unit_divisor=1024,
                    desc=filename,
                    bar_format='{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]',
                    colour='green',
                    ncols=100
                ) as pbar:
                    with open(destination, 'wb') as f:
                        for chunk in response.iter_content(chunk_size=8192):
                            if chunk:
                                f.write(chunk)
                                pbar.update(len(chunk))
                                
                                if progress_callback:
                                    progress_callback(pbar.n, file_size)
                
                self.log_handler.log(f"✓ Файл сохранен: {filename}", "OK")
                return True
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    self.log_handler.log(f"Ошибка: {e}. Повторяю...", "WARNING")
                else:
                    self.log_handler.log(f"Ошибка при скачивании {filename}: {e}", "ERROR")
                    return False
        
        return False
    
    def process_updates(self, game_path: str, skip_check: bool = False, progress_callback=None):
        """
        Основной процесс обновления игры
        
        Args:
            game_path: Путь к папке с игрой
            skip_check: Пропустить проверку файлов (скачать все)
            progress_callback: Функция для обновления общего прогресса
        """
        self.log_handler.log("="*60, "INFO")
        self.log_handler.log("Начало процесса обновления", "INFO")
        self.log_handler.log(f"Путь к игре: {game_path}", "INFO")
        self.log_handler.log("="*60, "INFO")
        
        # Получаем информацию о патчах
        patch_info = self.get_patch_info()
        
        # Удаляем файлы из списка на удаление
        files_to_delete = patch_info.get('FilesToDelete', [])
        if files_to_delete:
            self.log_handler.log(f"Удаление устаревших файлов ({len(files_to_delete)})...", "INFO")
            for file_path in files_to_delete:
                full_path = os.path.join(game_path, file_path)
                if os.path.exists(full_path):
                    try:
                        if os.path.isdir(full_path):
                            import shutil
                            shutil.rmtree(full_path)
                        else:
                            os.remove(full_path)
                        self.log_handler.log(f"Удален: {file_path}", "DELETED")
                    except Exception as e:
                        self.log_handler.log(f"Не удалось удалить {file_path}: {e}", "ERROR")
        
        # Проверяем какие файлы нужно скачать
        files_to_download = []
        files = patch_info.get('Files', [])
        
        self.log_handler.log("Проверка файлов...", "INFO")
        for file_info in files:
            if skip_check or self.check_file_needs_update(file_info, game_path):
                # Декодируем URL из base64 (поддержка обоих вариантов регистра)
                encoded_url = file_info.get('url', file_info.get('Url'))
                if encoded_url:
                    decoded_url = base64.b64decode(encoded_url).decode('ascii')
                    
                    # Нормализуем ключи к единому формату (с заглавной буквы)
                    normalized_info = {
                        'FileName': file_info.get('fileName', file_info.get('FileName')),
                        'Size': file_info.get('size', file_info.get('Size')),
                        'Hash': file_info.get('hash', file_info.get('Hash')),
                        'DecodedUrl': decoded_url
                    }
                    files_to_download.append(normalized_info)
                else:
                    self.log_handler.log(f"Пропущен файл без URL: {file_info.get('fileName', file_info.get('FileName', 'unknown'))}", "WARNING")
        
        if not files_to_download:
            self.log_handler.log("Все файлы актуальны! Обновление не требуется.", "OK")
            return
        
        # Скачиваем файлы
        total_size = sum(f['Size'] for f in files_to_download)
        self.log_handler.log(f"Найдено {len(files_to_download)} файлов для скачивания", "INFO")
        self.log_handler.log(f"Общий размер: {self._format_size(total_size)}", "INFO")
        
        downloaded = 0
        failed = []
        
        for i, file_info in enumerate(files_to_download, 1):
            self.log_handler.log(f"[{i}/{len(files_to_download)}]", "INFO")
            
            destination = os.path.join(game_path, file_info['FileName'])
            
            def file_progress(down, total):
                if progress_callback:
                    overall = ((i-1) / len(files_to_download)) * 100 + (down / total / len(files_to_download)) * 100
                    progress_callback(overall, f"{file_info['FileName']} ({down}/{total})")
            
            success = self.download_file(
                file_info['DecodedUrl'],
                destination,
                file_info['Size'],
                file_progress
            )
            
            if success:
                downloaded += 1
            else:
                failed.append(file_info['FileName'])
        
        # Итоги
        self.log_handler.log("="*60, "INFO")
        self.log_handler.log("Обновление завершено!", "OK")
        self.log_handler.log(f"Скачано: {downloaded}/{len(files_to_download)}", "INFO")
        if failed:
            self.log_handler.log(f"Ошибки: {len(failed)}", "ERROR")
            self.log_handler.log("Файлы с ошибками:", "ERROR")
            for f in failed:
                self.log_handler.log(f"  - {f}", "ERROR")
        self.log_handler.log("="*60, "INFO")
    
    def download_launcher_update(self, destination: str = "PandaWoWLauncher_new.exe", progress_callback=None, max_retries: int = 3):
        """
        Скачивание обновления лаунчера с повторными попытками
        
        Args:
            destination: Путь для сохранения нового лаунчера
            progress_callback: Функция для обновления прогресса
            max_retries: Максимальное количество попыток
        """
        # Получаем URL лаунчера из API если еще не получили
        if not self.launcher_url:
            self.log_handler.log("Получение URL лаунчера из API...", "INFO")
            self.get_patch_info()
        
        if not self.launcher_url:
            self.log_handler.log("Не удалось получить URL лаунчера из API", "ERROR")
            return False
        
        self.log_handler.log("Скачивание обновления лаунчера...", "INFO")
        
        for attempt in range(1, max_retries + 1):
            try:
                if attempt > 1:
                    self.log_handler.log(f"Попытка {attempt}/{max_retries}...", "INFO")
                
                # Увеличенный таймаут для больших файлов: 10 сек на подключение, 60 сек на чтение
                response = self.session.get(self.launcher_url, stream=True, timeout=(10, 60))
                response.raise_for_status()
                
                total_size = int(response.headers.get('content-length', 0))
                downloaded = 0
                
                self.log_handler.log(f"Размер файла: {self._format_size(total_size)}", "INFO")
                
                with open(destination, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)
                            downloaded += len(chunk)
                            if progress_callback:
                                progress_callback(downloaded, total_size)
                
                self.log_handler.log(f"Лаунчер сохранен: {destination}", "OK")
                return True
                
            except requests.exceptions.RequestException as e:
                if attempt < max_retries:
                    self.log_handler.log(f"Ошибка: {e}. Повторяю...", "WARNING")
                else:
                    self.log_handler.log(f"Ошибка при скачивании лаунчера: {e}", "ERROR")
                    return False
        
        return False
    
    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Форматирование размера в читаемый вид"""
        for unit in ['B', 'KB', 'MB', 'GB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} TB"


def main():
    """Главная функция"""
    import argparse
    
    # Путь по умолчанию - папка со скриптом (работает и для .exe)
    if getattr(sys, 'frozen', False):
        # Если запущен как .exe (PyInstaller)
        script_dir = os.path.dirname(sys.executable)
    else:
        # Если запущен как .py скрипт
        script_dir = os.path.dirname(os.path.abspath(__file__))
    
    default_game_path = script_dir
    
    parser = argparse.ArgumentParser(
        description='Скачивание файлов и обновлений PandaWoW с поддержкой SOCKS5 прокси',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Примеры использования:

  # Обновление игры с автоматическим выбором прокси (скачивает в папку со скриптом)
  python launcher.py

  # Обновление игры без прокси
  python launcher.py --no-proxy

  # Указать другой путь для игры
  python launcher.py --game-path "C:/Games/WoW"

  # Скачивание через конкретный SOCKS5 прокси
  python launcher.py --proxy socks5://127.0.0.1:1080

  # Принудительное скачивание всех файлов (без проверки)
  python launcher.py --force
        """
    )
    
    parser.add_argument(
        '--game-path',
        type=str,
        default=default_game_path,
        help='Путь к папке с игрой (по умолчанию: папка со скриптом)'
    )
    
    parser.add_argument(
        '--proxy',
        type=str,
        help='SOCKS5 прокси в формате socks5://[user:pass@]host:port'
    )
    
    parser.add_argument(
        '--no-proxy',
        action='store_true',
        help='Не использовать прокси (по умолчанию прокси выбирается автоматически)'
    )
    
    parser.add_argument(
        '--force',
        action='store_true',
        help='Принудительно скачать все файлы (без проверки)'
    )
    
    args = parser.parse_args()
    
    # Проверка зависимостей
    try:
        import requests
        import tqdm
    except ImportError as e:
        print("[ERROR] Не установлены необходимые библиотеки!")
        print("\nУстановите их командой:")
        print("  pip install -r requirements.txt")
        print("\nИли вручную:")
        print("  pip install requests requests[socks] tqdm")
        sys.exit(1)
    
    # Консольный режим
    log_handler = LogHandler()
    
    # Автоматический выбор прокси (если не указан --no-proxy и не указан конкретный прокси)
    proxy_url = args.proxy
    if not args.no_proxy and not args.proxy:
        temp_downloader = ProxyDownloader(log_handler=log_handler)
        # Проверяем ВСЕ прокси параллельно (100 потоков) до нахождения рабочего
        proxy_url = temp_downloader.auto_select_proxy(max_workers=100)
        # Если не нашли прокси, продолжаем без него (proxy_url будет None)
    
    # Создаем загрузчик
    downloader = ProxyDownloader(proxy_url=proxy_url, log_handler=log_handler)
    
    # Создаем папку если не существует
    if not os.path.exists(args.game_path):
        try:
            os.makedirs(args.game_path, exist_ok=True)
            log_handler.log(f"Создана папка: {args.game_path}", "INFO")
        except Exception as e:
            log_handler.log(f"Не удалось создать папку: {args.game_path}", "ERROR")
            log_handler.log(str(e), "ERROR")
            sys.exit(1)
    
    # Выполняем обновление
    try:
        downloader.process_updates(args.game_path, skip_check=args.force)
    except KeyboardInterrupt:
        log_handler.log("\nОбновление прервано пользователем", "WARNING")
        sys.exit(0)
    except Exception as e:
        log_handler.log(f"Критическая ошибка: {e}", "ERROR")
        sys.exit(1)


if __name__ == '__main__':
    main()
