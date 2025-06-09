# app.py

import os
from flask import Flask, render_template, request, jsonify, make_response
import requests
import json # JSON 데이터 처리를 위해
import config # config.py 파일 임포트

app = Flask(__name__)
# config.py에서 FLASK_SECRET_KEY를 로드하여 쿠키 서명에 사용합니다.
app.secret_key = config.FLASK_SECRET_KEY 

# Databricks Model Serving 엔드포인트 URL 및 헤더 설정
SERVING_URL = f"https://{config.SERVER_HOSTNAME}{config.HTTP_PATH}"
HEADERS = {
    "Authorization": f"Bearer {config.ACCESS_TOKEN}",
    "Content-Type": "application/json"
}

# 쿠키 이름 정의
INMV_COOKIE_NAME = "inmv_history"

@app.route('/')
def index():
    # 쿠키에서 이전에 입력했던 영화 ID 목록을 가져옵니다.
    # 쿠키 값은 쉼표로 구분된 문자열이거나, JSON 배열 문자열일 수 있습니다.
    # 여기서는 간단히 쉼표 구분 문자열로 가정합니다.
    current_inmv_str = request.cookies.get(INMV_COOKIE_NAME, "")
    return render_template('index.html', current_inmv=current_inmv_str)

@app.route('/recommend', methods=['POST'])
def recommend_movies():
    # 1. 사용자로부터 현재 입력된 영화 ID를 가져옵니다.
    new_input_movie_id_str = request.form.get('inmv')
    if not new_input_movie_id_str:
        return jsonify({"error": "영화 ID를 입력해야 합니다."}), 400

    # 2. 쿠키에서 기존에 입력했던 영화 ID를 가져옵니다.
    existing_inmv_str = request.cookies.get(INMV_COOKIE_NAME, "")

    # 3. 기존 ID와 새로운 ID를 합치고 중복을 제거합니다.
    all_inmv_ids = set()
    if existing_inmv_str:
        try:
            # 쉼표로 구분된 문자열을 정수 집합으로 변환
            all_inmv_ids.update(int(x.strip()) for x in existing_inmv_str.split(',') if x.strip())
        except ValueError:
            # 쿠키 값이 유효하지 않은 경우 무시하고 새 값만 사용
            app.logger.warning(f"Invalid existing inmv cookie value: {existing_inmv_str}")

    try:
        all_inmv_ids.update(int(x.strip()) for x in new_input_movie_id_str.split(',') if x.strip())
    except ValueError:
        return jsonify({"error": "유효하지 않은 영화 ID 형식이 포함되어 있습니다."}), 400
    
    # 병합된 영화 ID 목록을 다시 쉼표로 구분된 문자열로 변환 (정렬하여 일관성 유지)
    merged_inmv_str = ','.join(map(str, sorted(list(all_inmv_ids))))

    # 4. Databricks 모델에 보낼 입력 데이터 준비
    # 모델의 predict 메서드가 단일 movie_id를 받도록 가정한다면, 합쳐진 리스트의 첫 번째 ID를 사용합니다.
    # 모델이 여러 movie_id를 입력으로 받아들일 수 있다면, all_inmv_ids 전체를 보낼 수 있도록 payload를 수정해야 합니다.
    # 여기서는 가장 간단하게 합쳐진 리스트의 첫 번째 ID를 사용합니다.
    input_for_model = list(all_inmv_ids)[0] if all_inmv_ids else None

    if input_for_model is None:
        return jsonify({"error": "유효한 영화 ID가 없습니다. 적어도 하나의 영화 ID를 입력해주세요."}), 400

    # Databricks 모델에 전송할 페이로드 (모델의 입력 스키마에 맞춰야 합니다)
    payload = {
        "dataframe_split": {
            "columns": ["movie_id"], 
            "data": [[input_for_model]] # 예시: 단일 영화 ID를 입력으로 가정
        }
        # 만약 모델이 여러 영화 ID를 처리할 수 있다면, 아래와 같이 변경할 수 있습니다.
        # "dataframe_split": {
        #     "columns": ["movie_ids"], # 모델이 여러 ID를 받는 칼럼 이름
        #     "data": [[list(all_inmv_ids)]] # 전체 리스트를 보냄
        # }
    }

    recommended_movie_ids = []
    try:
        # 5. Databricks Model Serving API 호출
        response = requests.post(SERVING_URL, headers=HEADERS, json=payload)
        response.raise_for_status() # HTTP 오류 발생 시 예외 발생 (4xx 또는 5xx)
        
        recommendations_data = response.json()
        
        # Databricks 모델의 응답 형식에 따라 파싱
        # 예시 모델은 {"predictions": [[346, 3356]]} 형태를 반환한다고 가정
        recommended_movie_ids = recommendations_data.get("predictions", [[]])[0]
        outmv_str = ','.join(map(str, recommended_movie_ids)) # 쉼표로 구분된 문자열로 변환

    except requests.exceptions.RequestException as e:
        app.logger.error(f"Databricks Model Serving 호출 오류: {e}")
        return jsonify({"error": f"추천 모델 호출 오류: {e}. 상세: {e}"}), 500
    except Exception as e:
        app.logger.error(f"알 수 없는 오류 발생: {e}")
        return jsonify({"error": f"서버 오류: {e}"}), 500

    # 6. Flask 응답 생성 및 쿠키에 합쳐진 영화 ID 저장
    resp = make_response(jsonify({
        "input_movies": list(all_inmv_ids), # 현재 세션에서 입력된 모든 영화 ID
        "recommended_movies": recommended_movie_ids,
        "message": "영화 추천이 완료되었습니다."
    }))
    
    # 합쳐진 입력 영화 ID를 쿠키에 저장하여 다음 요청 시 활용합니다.
    # expires=None으로 설정하면 브라우저를 닫을 때까지 유효합니다.
    # max_age를 설정하여 영구적으로 저장할 수도 있습니다.
    resp.set_cookie(INMV_COOKIE_NAME, merged_inmv_str)

    return resp

if __name__ == '__main__':
    # 개발 환경에서만 debug=True로 설정합니다.
    # config.py의 FLASK_SECRET_KEY가 설정되어 있어야 합니다.
    app.run(debug=True)