import argparse
from collections import defaultdict
from typing import Dict, List
import random
import pandas as pd


class RecommendationEngine:
    def __init__(self, data_path: str):
        self.df = pd.read_csv(data_path)
        self._prepare_data()
    
    def _prepare_data(self):
        #Подготовка данных для рекомендаций.
        # Агрегируем данные по пользователям и товарам
        self.user_product_stats = self.df.groupby(['uid', 'pid', 'brand']).agg({
            'click': 'sum',
            'add_to_cart': 'sum',
            'purchase': 'sum'
        }).reset_index()

        # Популярность товаров (агрегация по всем пользователям)
        self.product_popularity = self.df.groupby(['pid', 'brand']).agg({
            'click': 'sum',
            'add_to_cart': 'sum',
            'purchase': 'sum'
        }).reset_index()

        coef_click = self.product_popularity['purchase'].sum() / max(self.product_popularity['click'].sum(), 1)
        coef_cart = self.product_popularity['purchase'].sum() / max(self.product_popularity['add_to_cart'].sum(), 1)
        coef_purchase = 1

        print(f"[DEBUG] Коэффициенты: click={coef_click:.3f}, add_to_cart={coef_cart:.3f}, purchase={coef_purchase}")

        # Рассчитываем нормализованный popularity_score
        self.product_popularity['popularity_score'] = (
            self.product_popularity['click'] * coef_click +
            self.product_popularity['add_to_cart'] * coef_cart +
            self.product_popularity['purchase'] * coef_purchase
        )
        
        # Сортируем по популярности
        self.product_popularity = self.product_popularity.sort_values(
            'popularity_score', ascending=False
        )
        
        # Создаём словарь pid -> brand для быстрого доступа
        self.product_brands = (
            self.df[['pid', 'brand']]
            .drop_duplicates('pid')
            .set_index('pid')['brand']
            .to_dict()
        )
        
        # Подготовка данных
        self._prepare_cooccurrence()
    
    def _prepare_cooccurrence(self):
        #Подготовка матрицы совместных покупок (co-occurrence).
        # Получаем покупки и товары из корзины
        purchases = self.user_product_stats[
            (self.user_product_stats['purchase'] > 0) |
            (self.user_product_stats['add_to_cart'] > 0)
        ][['uid', 'pid', 'brand']].copy()
        
        # Создаем словарь: для каждого товара список похожих товаров
        self.cooccurrence = defaultdict(list)

        # Группируем по пользователям
        user_purchases = purchases.groupby('uid')['pid'].apply(list).to_dict()

        cooccur_counts = defaultdict(lambda: defaultdict(int))
        
        for uid, products in user_purchases.items():
            # Для каждой пары товаров
            for i, pid1 in enumerate(products):
                for pid2 in products[i+1:]:
                    cooccur_counts[pid1][pid2] += 1
                    cooccur_counts[pid2][pid1] += 1
        
        # Преобразуем в отсортированные списки
        for pid, similar_products in cooccur_counts.items():
            # Группируем товары по частоте
            freq_dict = defaultdict(list)
            for p, freq in similar_products.items():
                freq_dict[freq].append(p)

            sorted_randomized = []
            for freq in sorted(freq_dict.keys(), reverse=True):
                products = freq_dict[freq]
                random.shuffle(products)
                sorted_randomized.extend(products)

            self.cooccurrence[pid] = sorted_randomized

    def get_recommendations_for_existing_user(self, uid: int) -> List[int]:
        user_data = self.user_product_stats[
            self.user_product_stats['uid'] == uid
        ].copy()

        if user_data.empty:
            return []

        # Все товары, с которыми пользователь взаимодействовал (клик/корзина/покупка)
        interested_products = set(
            user_data[
                (user_data['add_to_cart'] > 0) |
                (user_data['click'] > 0) |
                (user_data['purchase'] > 0)
            ]['pid']
        )

        print(f"[INFO] uid={uid} | interested_products={len(interested_products)}")

        recommendations: List[int] = []

        # 1) Аггрегируем co-occurrence по всем интересным товарам с подсчетом частоты
        candidate_counts = defaultdict(int)
        for seed_pid in interested_products:
            if seed_pid in self.cooccurrence:
                for similar_pid in self.cooccurrence[seed_pid]:
                    if similar_pid not in interested_products:
                        candidate_counts[similar_pid] += 1

        # Сортируем кандидатов по убыванию встречаемости (релевантности)
        ranked_candidates = sorted(
            candidate_counts.items(), key=lambda x: x[1], reverse=True
        )

        seen_items = set()  
        for pid, count in ranked_candidates:
            brand = self.product_brands.get(pid, 'unknown')
            if (pid, brand) in seen_items:
                continue
            recommendations.append(pid)
            seen_items.add((pid, brand))
            print(f"[INFO] uid={uid} | add cooccur pid={pid} brand={brand} freq={count}")
            if len(recommendations) >= 5:
                break

        # 2) Фоллбек: добавляем популярные товары, если меньше 5
        if len(recommendations) < 5:
            recommendations = self.fill_with_popular_products(
                recommendations, 
                interested_products=interested_products,
                uid=uid
            )

        return recommendations
    
    def fill_with_popular_products(
        self, 
        recommendations: List[int], 
        max_count: int = 5, 
        brand_limit: int = 2,
        interested_products: set = None,
        uid: int = None
    ) -> List[int]:
        seen_items = set()  
        
        
        for pid in recommendations:
            brand = self.product_brands.get(pid, 'unknown')
            seen_items.add((pid, brand))
        
        if interested_products is None:
            interested_products = set()
        
        for row in self.product_popularity.itertuples(index=False):
            pid, brand = row.pid, row.brand
            if pid in interested_products:
                continue
            if (pid, brand) in seen_items:
                continue
            recommendations.append(pid)
            seen_items.add((pid, brand))
            if uid is not None:
                print(f"[INFO] uid={uid} | add popular pid={pid} brand={brand}")
            if len(recommendations) >= max_count:
                break
        
        return recommendations
    
    def get_recommendations(self, uid: int) -> Dict[str, any]:
        # Проверяем, есть ли пользователь в истории
        user_exists = uid in self.user_product_stats['uid'].values
        recommendations: List[int] = []
        
        if user_exists:
            recommendations = self.get_recommendations_for_existing_user(uid)
        else:
            # Для новых пользователей сразу популярные товары
            recommendations = self.fill_with_popular_products(recommendations, uid=uid)
        
        return {
            "uid": uid,
            "products": recommendations[:5]  
        }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run recommendations from console")
    parser.add_argument("--uid", type=int, help="User id for recommendations")
    parser.add_argument("--data", type=str, default="data.csv", help="Path to CSV data file")
    parser.add_argument("--print-cooccurrence", action="store_true", help="Print truncated co-occurrence matrix")
    parser.add_argument("--max-seeds", type=int, default=10, help="How many seed products to show in co-occurrence print")
    parser.add_argument("--max-sims", type=int, default=5, help="How many similar products to show per seed")
    args = parser.parse_args()

    uid = args.uid
    if uid is None:
        try:
            uid = int(input("Enter user id (uid): "))
        except Exception:
            print("uid is required")
            raise SystemExit(1)

    engine = RecommendationEngine(args.data)

    if args.print_cooccurrence:
        print("=== Co-occurrence (truncated) ===")
        seeds = list(engine.cooccurrence.items())
        for pid, sims in seeds[:args.max_seeds]:
            print(f"seed {pid}: {sims[:args.max_sims]}")
        if len(seeds) > args.max_seeds:
            print(f"... truncated {len(seeds) - args.max_seeds} more seeds")

    recs = engine.get_recommendations(uid)
    print("=== Recommendations ===")
    print(recs)
