# app.py

import os
from flask import Flask, render_template, request, jsonify, make_response
import requests
import json
import config
from databricks import sql

app = Flask(__name__)
app.secret_key = config.FLASK_SECRET_KEY

# Databricks Model Serving 엔드포인트 URL 및 헤더 설정
SERVING_URL = f"https://{config.SERVER_HOSTNAME}{config.HTTP_PATH}"
HEADERS = {
    "Authorization": f"Bearer {config.ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# TMDB API 설정
# TMDB_API_KEY 대신 TMDB_ACCESS_TOKEN을 사용합니다.
TMDB_ACCESS_TOKEN = config.TMDB_ACCESS_TOKEN
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500" # 포스터 이미지 크기 설정

# 쿠키 이름 정의
INMV_COOKIE_NAME = "inmv_history"

def get_tmdb_id_from_movie_id(movie_id):
    """
    주어진 movieId에 해당하는 tmdbId를 Databricks에서 조회합니다.
    """
    try:
        with sql.connect(
            server_hostname=config.SERVER_HOSTNAME,
            http_path=config.DB_HTTP_PATH,
            access_token=config.ACCESS_TOKEN
        ) as connection:
            cursor = connection.cursor()
            query = f"SELECT tmdbId FROM `1dt_team8_databricks`.`movielens-small`.links WHERE movieId = {movie_id}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0]  # tmdbId 반환
            else:
                app.logger.warning(f"movieId {movie_id}에 대한 tmdbId를 찾을 수 없습니다.")
                return None
    except Exception as e:
        app.logger.error(f"Databricks SQL 연결 또는 쿼리 오류: {e}")
        return None

def get_movie_details_from_tmdb(tmdb_id):
    """
    TMDB API를 사용하여 영화의 포스터와 한글 제목을 가져옵니다.
    """
    details_url = f"{TMDB_BASE_URL}/movie/{tmdb_id}"
    tmdb_headers = { # TMDB API를 위한 별도의 헤더
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}"
    }
    params = {
        "language": "ko-KR" # 한글 제목을 위해 ko-KR 설정
    }
    try:
        response = requests.get(details_url, headers=tmdb_headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        poster_path = data.get("poster_path")
        # 응답 예시에서 'title'이 한글 제목입니다.
        title_ko = data.get("title") 

        poster_url = f"{TMDB_IMAGE_BASE_URL}{poster_path}" if poster_path else None
        
        return {
            "tmdb_id": tmdb_id,
            "title_ko": title_ko,
            "poster_url": poster_url
        }
    except requests.exceptions.RequestException as e:
        app.logger.error(f"TMDB API 호출 오류 (TMDB ID: {tmdb_id}): {e}. 응답: {e.response.text if e.response else 'N/A'}")
        return None
    except Exception as e:
        app.logger.error(f"TMDB 상세 정보 파싱 오류 (TMDB ID: {tmdb_id}): {e}")
        return None

@app.route('/')
def index():
    current_inmv_str = request.cookies.get(INMV_COOKIE_NAME, "")
    return render_template('index.html', current_inmv=current_inmv_str)

@app.route('/recommend', methods=['POST'])
def recommend_movies():
    new_input_movie_id_str = request.form.get('inmv')
    if not new_input_movie_id_str:
        return jsonify({"error": "영화 ID를 입력해야 합니다."}), 400

    existing_inmv_str = request.cookies.get(INMV_COOKIE_NAME, "")

    all_inmv_ids = set()
    if existing_inmv_str:
        try:
            all_inmv_ids.update(int(x.strip()) for x in existing_inmv_str.split(',') if x.strip())
        except ValueError:
            app.logger.warning(f"Invalid existing inmv cookie value: {existing_inmv_str}")

    try:
        all_inmv_ids.update(int(x.strip()) for x in new_input_movie_id_str.split(',') if x.strip())
    except ValueError:
        return jsonify({"error": "유효하지 않은 영화 ID 형식이 포함되어 있습니다."}), 400

    merged_inmv_str = ','.join(map(str, sorted(list(all_inmv_ids))))

    # --- 요청 형식 변경 시작 ---
    # `movie_history` 리스트를 직접 JSON payload에 포함합니다.
    movie_history_list = list(all_inmv_ids)
    if not movie_history_list:
        return jsonify({"error": "유효한 영화 ID가 없습니다. 적어도 하나의 영화 ID를 입력해주세요."}), 400

    payload = {
        "inputs": {
            "movie_history": movie_history_list # 요청 형식에 맞게 movie_history 리스트 전달
        }
    }
    # --- 요청 형식 변경 끝 ---

    recommended_movie_tmdb_ids = [] # TMDB ID를 저장할 리스트
    try:
        response = requests.post(SERVING_URL, headers=HEADERS, json=payload)
        response.raise_for_status()

        recommendations_data = response.json()

        # --- 응답 파싱 형식 변경 시작 ---
        # "predictions" 키 아래에 있는 리스트의 첫 번째 요소에서 "recommendations" 키를 찾습니다.
        if "predictions" in recommendations_data and isinstance(recommendations_data["predictions"], list) and len(recommendations_data["predictions"]) > 0:
            first_prediction = recommendations_data["predictions"][0]
            if "recommendations" in first_prediction and isinstance(first_prediction["recommendations"], list):
                model_recommended_movie_ids = first_prediction["recommendations"]
                # movie_id를 tmdbId로 변환
                for movie_id in model_recommended_movie_ids:
                    tmdb_id = get_tmdb_id_from_movie_id(movie_id)
                    if tmdb_id is not None:
                        recommended_movie_tmdb_ids.append(tmdb_id)
                    else:
                        app.logger.warning(f"모델 추천 movieId {movie_id}에 대한 tmdbId를 찾을 수 없어 TMDB 정보 조회를 건너뜝니다.")
            else:
                app.logger.error("Databricks Model Serving 응답에 'recommendations' 키가 없거나 형식이 올바르지 않습니다.")
                return jsonify({"error": "추천 모델 응답 형식이 예상과 다릅니다."}), 500
        else:
            app.logger.error("Databricks Model Serving 응답에 'predictions' 키가 없거나 형식이 올바르지 않습니다.")
            return jsonify({"error": "추천 모델 응답 형식이 예상과 다릅니다."}), 500
        # --- 응답 파싱 형식 변경 끝 ---

        # TMDB API를 통해 영화 상세 정보 가져오기
        recommended_movie_details = []
        for tmdb_id in recommended_movie_tmdb_ids:
            details = get_movie_details_from_tmdb(tmdb_id)
            if details:
                recommended_movie_details.append(details)
            else:
                app.logger.warning(f"TMDB ID {tmdb_id}에 대한 상세 정보를 가져올 수 없습니다.")

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Databricks Model Serving 호출 오류: {e}. 응답: {e.response.text if e.response else 'N/A'}")
        return jsonify({"error": f"추천 모델 호출 오류: {e}. 상세: {e.response.text if e.response else 'API 연결 실패'}"}), 500
    except Exception as e:
        app.logger.error(f"알 수 없는 오류 발생: {e}")
        return jsonify({"error": f"서버 오류: {e}"}), 500

    resp = make_response(jsonify({
        "input_movies": list(all_inmv_ids),
        "recommended_movies": recommended_movie_details, # TMDB 상세 정보를 포함한 리스트 전달
        "message": "영화 추천이 완료되었습니다."
    }))

    resp.set_cookie(INMV_COOKIE_NAME, merged_inmv_str)

    return resp

if __name__ == '__main__':
    app.run(debug=True)