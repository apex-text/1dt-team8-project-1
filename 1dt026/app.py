from flask import Flask, render_template
from databricks import sql
import pandas as pd
import config # config.py 파일에서 설정 정보를 가져옵니다.


app = Flask(__name__)
app.run(host='0.0.0.0')
@app.route('/')
def index():
    return render_template('index.html', result_df=None)

@app.route('/get_data', methods=['POST'])
def get_data():
    df_html = None
    try:
        connection = sql.connect(
            config.SERVER_HOSTNAME,
            config.HTTP_PATH,
            config.ACCESS_TOKEN
        )
        
        # SQL 쿼리를 실행하고 결과를 DataFrame으로 가져옵니다.
        df = pd.read_sql("SELECT * from `1dt_team8_databricks`.`movielens-small`.links LIMIT 10", connection)
        
        # DataFrame을 HTML 테이블로 변환합니다.
        # to_html() 메소드에 'classes' 인자를 사용하여 CSS 스타일을 적용할 수 있습니다.
        df_html = df.to_html(classes='table table-striped', index=False) # index=False로 설정하면 DataFrame 인덱스를 제외합니다.
        
        connection.close()

    except Exception as e:
        df_html = f"<p style='color:red;'>데이터를 가져오는 중 오류가 발생했습니다: {e}</p>"
    
    return render_template('index.html', result_df=df_html)

if __name__ == '__main__':
    app.run(debug=True)