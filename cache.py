import json
import os
try:
    import redis
except ImportError:
    redis = None


class CacheManager:
    def __init__(self, host=None, port=None, db=0):
        """Инициализация подключения к KeyDB (или Redis)."""
        if redis is None:
            print(f"⚠️  Библиотека redis не установлена. Кэш отключен.")
            self.kdb = None
            return
            
        # Используем переменные окружения если не переданы параметры
        host = host or os.getenv('KEYDB_HOST', 'localhost')
        port = port or int(os.getenv('KEYDB_PORT', 6379))
        
        try:
            self.kdb = redis.Redis(host=host, port=port, db=db, decode_responses=True)
            self.kdb.ping()
            print(f"✅ Подключение к KeyDB успешно ({host}:{port})")
        except Exception as e:
            print(f"⚠️  Не удалось подключиться к KeyDB: {e}. Кэш отключен.")
            self.kdb = None

    def is_available(self):
        """Проверка доступности кэша."""
        return self.kdb is not None

    def save_cooccurrence(self, cooccurrence: dict):
        """Сохраняет co-occurrence матрицу в KeyDB."""
        if not self.is_available():
            return
        try:
            # Сохраняем всю матрицу как один ключ для быстрой загрузки
            self.kdb.set("cooccurrence_full", json.dumps(cooccurrence))
            print(f"[CACHE] Сохранено {len(cooccurrence)} co-occurrence записей")
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при сохранении co-occurrence: {e}")
    
    def load_cooccurrence(self):
        """Загружает всю co-occurrence матрицу из KeyDB."""
        if not self.is_available():
            return None
        try:
            data = self.kdb.get("cooccurrence_full")
            if data:
                cooc_dict = json.loads(data)
                # Преобразуем ключи обратно в int
                return {int(k): v for k, v in cooc_dict.items()}
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при загрузке cooccurrence: {e}")
        return None

    def save_popular_products(self, popular_list: list):
        """Сохраняет список популярных товаров."""
        if not self.is_available():
            return
        try:
            self.kdb.set("popular_products", json.dumps(popular_list))
            print(f"[CACHE] Сохранено {len(popular_list)} популярных товаров")
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при сохранении popular_products: {e}")
    
    def load_popular_products(self):
        """Загружает список популярных товаров."""
        if not self.is_available():
            return None
        try:
            data = self.kdb.get("popular_products")
            if data:
                return json.loads(data)
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при загрузке popular_products: {e}")
        return None

    def save_product_brands(self, brands: dict):
        """Сохраняет словарь pid → brand."""
        if not self.is_available():
            return
        try:
            brands_str = {str(k): v for k, v in brands.items()}
            self.kdb.set("product_brands", json.dumps(brands_str))
            print(f"[CACHE] Сохранено {len(brands)} записей product_brands")
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при сохранении product_brands: {e}")

    def load_product_brands(self):
        """Загружает словарь pid → brand."""
        if not self.is_available():
            return {}
        try:
            data = self.kdb.get("product_brands")
            if data:
                brands_str = json.loads(data)
                return {int(k): v for k, v in brands_str.items()}
        except Exception as e:
            print(f"[CACHE ERROR] Ошибка при загрузке product_brands: {e}")
        return {}

