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
        
        self.product_popularity['popularity_score'] = (
            self.product_popularity['click'] * 1 +
            self.product_popularity['add_to_cart'] * 3 +
            self.product_popularity['purchase'] * 5
        )
        
        # Сортируем по популярности
        self.product_popularity = self.product_popularity.sort_values(
            'popularity_score', ascending=False
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

        # Для каждой пары товаров, купленных одним пользователем,
        # увеличиваем счетчик совместных покупок
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

        product_brands = (
            self.df[['pid', 'brand']]
            .drop_duplicates('pid')
            .set_index('pid')['brand']
            .to_dict()
        )

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
        brand_count = defaultdict(int)

        # 1) Аггрегируем co-occurrence по всем интересным товарам с подсчетом частоты
        candidate_counts = defaultdict(int)
        for seed_pid in interested_products:
            if seed_pid in self.cooccurrence:
                for similar_pid in self.cooccurrence[seed_pid]:
                    if similar_pid in interested_products:
                        continue  # не показываем то, что уже видел/брал
                    candidate_counts[similar_pid] += 1

        # Сортируем кандидатов по убыванию встречаемости (релевантности)
        ranked_candidates = sorted(
            candidate_counts.items(), key=lambda x: x[1], reverse=True
        )

        for pid, count in ranked_candidates:
            brand = product_brands.get(pid, 'unknown')
            if brand_count[brand] >= 2:
                continue
            if pid in recommendations:
                continue
            recommendations.append(pid)
            brand_count[brand] += 1
            print(f"[INFO] uid={uid} | add cooccur pid={pid} brand={brand} freq={count}")
            if len(recommendations) >= 5:
                break

        # 2) Фоллбек: добавляем популярные товары, избегая интересных, с лимитом 2 на бренд
        if len(recommendations) < 5:
            for _, row in self.product_popularity.iterrows():
                pid, brand = row['pid'], row['brand']
                if pid in interested_products:
                    continue
                if pid in recommendations:
                    continue
                if brand_count[brand] >= 1:
                    continue
                recommendations.append(pid)
                brand_count[brand] += 1
                print(f"[INFO] uid={uid} | add popular pid={pid} brand={brand}")
                if len(recommendations) >= 5:
                    break

        return recommendations
    
    def get_recommendations(self, uid: int) -> Dict[str, any]:
        # Проверяем, есть ли пользователь в истории
        user_exists = uid in self.user_product_stats['uid'].values
        
        if user_exists:
            recommendations = self.get_recommendations_for_existing_user(uid)
        else:

            recommendations = []
        
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
