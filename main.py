"""
HTTP API для системы рекомендаций товаров.
"""

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import JSONResponse, HTMLResponse
from recommendation_engine import RecommendationEngine
import os

app = FastAPI(
    title="Product Recommendation API",
    description="API для получения рекомендаций товаров на основе истории взаимодействий пользователей",
    version="1.0.0"
)

# Путь к файлу с данными
DATA_FILE = "data.csv"

# Инициализация движка рекомендаций
try:
    if not os.path.exists(DATA_FILE):
        print(f"⚠️  Предупреждение: Файл {DATA_FILE} не найден.")
        print(f"   Пожалуйста, поместите CSV файл с данными в корень проекта.")
        engine = None
    else:
        engine = RecommendationEngine(DATA_FILE)
        print(f"✅ Движок рекомендаций успешно инициализирован с файлом {DATA_FILE}")
except Exception as e:
    print(f"❌ Ошибка при инициализации движка рекомендаций: {e}")
    engine = None


@app.get("/")
async def root():
    """Корневой endpoint с информацией об API."""
    return {
        "message": "Product Recommendation API",
        "version": "1.0.0",
        "endpoints": {
            "/recommendations": "GET /recommendations?user_id=<uid> - Получить рекомендации для пользователя",
            "/docs": "Интерактивная документация API",
            "/health": "Проверка статуса API"
        }
    }


@app.get("/health")
async def health_check():
    """Проверка работоспособности API."""
    if engine is None:
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "message": f"Движок рекомендаций не инициализирован. Проверьте наличие файла {DATA_FILE}"
            }
        )
    
    return {
        "status": "healthy",
        "message": "API работает нормально"
    }


@app.get("/recommendations")
async def get_recommendations(
    user_id: int = Query(..., description="ID пользователя", example=123)
):
    """
    - **user_id**: Уникальный идентификатор пользователя
    
    Возвращает:
    - uid: ID пользователя
    - products: Список из максимум 5 рекомендованных товаров (product IDs)
    """
    if engine is None:
        raise HTTPException(
            status_code=503,
            detail=f"Сервис недоступен. Файл данных {DATA_FILE} не найден или не загружен."
        )
    
    try:
        recommendations = engine.get_recommendations(user_id)
        return recommendations
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Ошибка при генерации рекомендаций: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
