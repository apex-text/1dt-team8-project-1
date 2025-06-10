# app.py

import os
from flask import Flask, render_template, request, jsonify, make_response
import requests
import json
import config
from databricks import sql

app = Flask(__name__)

# Databricks Model Serving 엔드포인트 URL 및 헤더 설정
SERVING_URL = f"https://{config.SERVER_HOSTNAME}{config.HTTP_PATH}"
HEADERS = {
    "Authorization": f"Bearer {config.ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# TMDB API 설정
TMDB_ACCESS_TOKEN = config.TMDB_ACCESS_TOKEN
TMDB_BASE_URL = "https://api.themoviedb.org/3"
TMDB_IMAGE_BASE_URL = "https://image.tmdb.org/t/p/w500" # 포스터 이미지 크기 설정

def get_tmdb_id_from_movie_id(movie_id):
    """
    주어진 MovieLens movieId에 해당하는 tmdbId를 Databricks에서 조회합니다.
    """
    try:
        with sql.connect(
            server_hostname=config.SERVER_HOSTNAME,
            http_path=config.DB_HTTP_PATH,
            access_token=config.ACCESS_TOKEN
        ) as connection:
            cursor = connection.cursor()
            query = f"SELECT tmdbId FROM `1dt_team8_databricks`.`movielens-32m`.links WHERE movieId = {movie_id}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0]  # tmdbId 반환
            else:
                app.logger.warning(f"MovieLens movieId {movie_id}에 대한 tmdbId를 찾을 수 없습니다.")
                return None
    except Exception as e:
        app.logger.error(f"Databricks SQL 연결 또는 쿼리 오류 (MovieLens movieId -> tmdbId): {e}")
        return None

def get_movie_id_from_tmdb_id(tmdb_id):
    """
    주어진 tmdbId에 해당하는 MovieLens movieId를 Databricks에서 조회합니다.
    """
    try:
        with sql.connect(
            server_hostname=config.SERVER_HOSTNAME,
            http_path=config.DB_HTTP_PATH,
            access_token=config.ACCESS_TOKEN
        ) as connection:
            cursor = connection.cursor()
            query = f"SELECT movieId FROM `1dt_team8_databricks`.`movielens-32m`.links WHERE tmdbId = {tmdb_id}"
            cursor.execute(query)
            result = cursor.fetchone()
            cursor.close()
            if result:
                return result[0]  # MovieLens movieId 반환
            else:
                app.logger.warning(f"tmdbId {tmdb_id}에 대한 MovieLens movieId를 찾을 수 없습니다.")
                return None
    except Exception as e:
        app.logger.error(f"Databricks SQL 연결 또는 쿼리 오류 (tmdbId -> MovieLens movieId): {e}")
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
        title_ko = data.get("title") 

        poster_url = f"{TMDB_IMAGE_BASE_URL}{poster_path}" if poster_path else None
        
        return {
            "tmdb_id": tmdb_id,
            "title_ko": title_ko,
            "poster_url": poster_url,
            "release_date": data.get("release_date", "") # 개봉일 추가
        }
    except requests.exceptions.RequestException as e:
        app.logger.error(f"TMDB API 호출 오류 (TMDB ID: {tmdb_id}): {e}. 응답: {e.response.text if e.response else 'N/A'}")
        return None
    except Exception as e:
        app.logger.error(f"TMDB 상세 정보 파싱 오류 (TMDB ID: {tmdb_id}): {e}")
        return None

@app.route('/')
def index():
    return render_template('index.html', current_inmv_details=[])

@app.route('/search_movies', methods=['GET'])
def search_movies():
    query = request.args.get('query', '')
    if not query:
        return jsonify([])

    search_url = f"{TMDB_BASE_URL}/search/movie"
    tmdb_headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {TMDB_ACCESS_TOKEN}"
    }
    params = {
        "query": query,
        "language": "ko-KR" # 한글 결과 선호
    }

    try:
        response = requests.get(search_url, headers=tmdb_headers, params=params)
        response.raise_for_status()
        data = response.json()
        
        results = []
        for movie in data.get("results", [])[:10]: # 최대 10개 결과 반환
            tmdb_id = movie.get("id")
            title = movie.get("title")
            release_date = movie.get("release_date", "")
            poster_path = movie.get("poster_path")
            poster_url = f"{TMDB_IMAGE_BASE_URL}{poster_path}" if poster_path else None
            
            results.append({
                "tmdb_id": tmdb_id, 
                "title": title,
                "release_date": release_date,
                "poster_url": poster_url
            })
        return jsonify(results)

    except requests.exceptions.RequestException as e:
        app.logger.error(f"TMDB 영화 검색 API 호출 오류: {e}. 응답: {e.response.text if e.response else 'N/A'}")
        return jsonify({"error": "영화 검색 중 오류가 발생했습니다."}), 500
    except Exception as e:
        app.logger.error(f"알 수 없는 오류 발생 (영화 검색): {e}")
        return jsonify({"error": f"서버 오류: {e}"}), 500

@app.route('/get_movielens_id', methods=['GET'])
def get_movielens_id_for_tmdb():
    tmdb_id = request.args.get('tmdb_id', type=int)
    if not tmdb_id:
        return jsonify({"error": "TMDB ID가 필요합니다."}), 400
    
    movielens_id = get_movie_id_from_tmdb_id(tmdb_id)
    if movielens_id is not None:
        return jsonify({"movielens_id": movielens_id})
    else:
        return jsonify({"error": f"TMDB ID {tmdb_id}에 해당하는 MovieLens ID를 찾을 수 없습니다."}), 404

@app.route('/recommend', methods=['POST'])
def recommend_movies():
    selected_movielens_ids_str = request.form.get('selected_movielens_ids')
    
    try:
        all_inmv_ids = [int(x.strip()) for x in selected_movielens_ids_str.split(',') if x.strip()]
    except ValueError:
        return jsonify({"error": "유효하지 않은 영화 ID 형식이 포함되어 있습니다."}), 400
    
    movie_history_list = sorted(list(set(all_inmv_ids)))

    # 최소 3개 이상의 영화가 입력되었는지 확인
    if len(movie_history_list) < 2:
        return jsonify({"error": f"최소 2개 이상의 영화를 선택해야 추천을 받을 수 있습니다. 현재 {len(movie_history_list)}개 선택됨."}), 400

    payload = {
        "inputs": {
            "movie_history": movie_history_list
        }
    }

    recommended_movie_tmdb_ids = []
    try:
        response = requests.post(SERVING_URL, headers=HEADERS, json=payload)
        response.raise_for_status()

        recommendations_data = response.json()

        if "predictions" in recommendations_data and isinstance(recommendations_data["predictions"], list) and len(recommendations_data["predictions"]) > 0:
            first_prediction = recommendations_data["predictions"][0]
            if "recommendations" in first_prediction and isinstance(first_prediction["recommendations"], list):
                model_recommended_movie_ids = first_prediction["recommendations"]
                for movie_id in model_recommended_movie_ids:
                    tmdb_id = get_tmdb_id_from_movie_id(movie_id)
                    if tmdb_id is not None:
                        recommended_movie_tmdb_ids.append(tmdb_id)
                    else:
                        app.logger.warning(f"모델 추천 MovieLens movieId {movie_id}에 대한 tmdbId를 찾을 수 없어 TMDB 정보 조회를 건너뜝니다.")
            else:
                app.logger.error("Databricks Model Serving 응답에 'recommendations' 키가 없거나 형식이 올바르지 않습니다.")
                return jsonify({"error": "추천 모델 응답 형식이 예상과 다릅니다."}), 500
        else:
            app.logger.error("Databricks Model Serving 응답에 'predictions' 키가 없거나 형식이 올바르지 않습니다.")
            return jsonify({"error": "추천 모델 응답 형식이 예상과 다릅니다."}), 500

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

    current_input_movie_details = []
    for ml_id in movie_history_list:
        tmdb_id = get_tmdb_id_from_movie_id(ml_id)
        if tmdb_id:
            details = get_movie_details_from_tmdb(tmdb_id)
            if details:
                details['movie_id'] = ml_id # MovieLens ID 추가
                current_input_movie_details.append(details)
            else:
                current_input_movie_details.append({"movie_id": ml_id, "title_ko": f"ID {ml_id} (정보 없음)", "poster_url": None, "tmdb_id": tmdb_id})
        else:
            current_input_movie_details.append({"movie_id": ml_id, "title_ko": f"ID {ml_id} (TMDB ID 없음)", "poster_url": None, "tmdb_id": None})


    resp = make_response(jsonify({
        "input_movies": current_input_movie_details,
        "recommended_movies": recommended_movie_details,
        "message": "영화 추천이 완료되었습니다."
    }))

    return resp

if __name__ == '__main__':
    app.run(debug=True)