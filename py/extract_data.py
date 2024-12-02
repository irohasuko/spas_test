import sys
sys.path.append('./site-packages')
import pyodbc
import pandas as pd

DRIVER = '{SQL Server}'
SERVER = 'HP-ENVY'
DATABASE = 'MANAGE_TEST'
TRUSTED_CONNECTION ='yes'
SQL_SELECT = "SELECT SHC_CD, DEP_CD, CLS_CD, SHC_NAME, DEP_NAME, CLS_NAME, CONCAT(SHC_CD, DEP_CD, CLS_CD) AS 比較用コード FROM SCHOOL_INFO"
    
URL = 'DRIVER='+DRIVER+';SERVER='+SERVER+';DATABASE='+DATABASE+';PORT=1433;Trusted_Connection='+TRUSTED_CONNECTION+';'

def get_current_data():
    """現在のDBデータを取得して配列で返却する
    """
    conn = pyodbc.connect(URL)
    cursor = conn.cursor()
    cursor.execute('SELECT * FROM Inquiry')
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    
    print(rows)
    
    return rows

def get_gaisan():
    """現在のDBデータを取得して配列で返却する
    """
    conn = pyodbc.connect(URL)
    cursor = conn.cursor()
    
    # 資料請求情報の取得
    SIRYO_SQL = """
SELECT 
    NOW_GRADE, 
    P_CD, 
    K_DIC, 
    COUNT(1) AS cnt 
FROM (
    SELECT 
        R_YEAR, 
        R_GRADE, 
        2022 - CAST(R_YEAR AS INT) + CAST(R_GRADE AS INT) AS NOW_GRADE, 
        P_CD, 
        K_DIC
    FROM CustomerInfo AS CI
    INNER JOIN Inquiry AS I ON CI.P_ID = I.P_ID
    INNER JOIN HighSchoolMaster AS HM ON CI.K_CD = HM.K_CD
) AS subquery
GROUP BY 
    R_YEAR, 
    R_GRADE, 
    P_CD, 
    K_DIC, 
    NOW_GRADE;
    """
    
    df = pd.read_sql(SIRYO_SQL, conn)

    cursor.close()
    conn.close()
    
    print(df)
    
    df = df.pivot_table(index='P_CD', columns='K_DIC', values='cnt', aggfunc='sum')
    
    print(df)
    
    # データフレームの作成
    data = {
        "NOW_GRADE": [3, 4, 2, 2, 3],
        "P_CD": [23, 13, 13, 27, 13],
        "K_DIC": ["C2", "A1", "A1", "A1", "A1"],
        "cnt": [1, 5, 1, 1, 1],
    }
    df = pd.DataFrame(data)

    # 常に表示したい K_DIC のリスト
    fixed_columns = ["A1", "B2", "C2"]

    # P_CD ごとに K_DIC をカウント集計
    grouped = df.groupby(["P_CD", "K_DIC"])["cnt"].sum().unstack(fill_value=0)

    # 固定列を反映し、欠けている列を 0 で補完
    for col in fixed_columns:
        if col not in grouped.columns:
            grouped[col] = 0

    # 列の順序を固定列順に整える
    grouped = grouped[fixed_columns]

    # 各行の合計（新しい列 "Row_Total" を追加）
    grouped["Row_Total"] = grouped.sum(axis=1)

    # 各列の合計（新しい行 "Column_Total" を追加）
    grouped.loc["Column_Total"] = grouped.sum()

    # 結果をリセットインデックスして整形
    result = grouped.reset_index()

    print(result)
    
    
    return df

def conma_test():
    conn = pyodbc.connect(URL)
    cursor = conn.cursor()
    
    # 資料請求情報の取得
    SIRYO_SQL = """
        SELECT P_ID, U_CD FROM Inquiry
    """
    
    df = pd.read_sql(SIRYO_SQL, conn)

    cursor.close()
    conn.close()
    
    result = df.groupby('P_ID')['U_CD'].agg(', '.join).reset_index()
    
    return result
    
def res_test():
    data = [
        {"P_ID": "P0001", "RES_CD": "R004"},
        {"P_ID": "P0001", "RES_CD": "R005"},
        {"P_ID": "P0001", "RES_CD": "R001"},
        {"P_ID": "P0001", "RES_CD": "R006"},
        {"P_ID": "P0001", "RES_CD": "R007"},
        {"P_ID": "P0002", "RES_CD": "R003"},
        {"P_ID": "P0002", "RES_CD": "R002"},
        {"P_ID": "P0003", "RES_CD": "R008"},
        {"P_ID": "P0003", "RES_CD": "R009"},
        {"P_ID": "P0003", "RES_CD": "R001"},
        {"P_ID": "P0004", "RES_CD": "R004"},
        {"P_ID": "P0005", "RES_CD": "R001"}
    ]

    df = pd.DataFrame(data)

    # P_IDごとにRES_CDを昇順にソートしてリスト化
    grouped = (
        df.sort_values(by="RES_CD")  # RES_CDを昇順にソート
        .groupby("P_ID")["RES_CD"]  # P_IDごとにグループ化
        .apply(lambda x: x.head(3).tolist())  # 各グループで最大3つをリスト化
        .reset_index()  # インデックスをリセット
    )

    # リストを列に展開し、列名を「資料1」「資料2」「資料3」に設定
    result = grouped["RES_CD"].apply(pd.Series)
    result.columns = ["資料1", "資料2", "資料3"]

    # P_IDを結合
    result.insert(0, "P_ID", grouped["P_ID"])

    # 結果を表示
    return result

def describe(count):
    df = pd.DataFrame(count)
    print(df)

count = get_gaisan()

# a = conma_test()
# b = res_test()

# data = pd.merge(a, b, on="P_ID", how="inner")

# data.to_excel('test.xlsx', index=False)