import pyodbc
import pandas as pd

import constants

db_connection = None

def initialize_db_connection():
    """データベース接続を初期化"""
    global db_connection
    if db_connection is None:
        db_connection = pyodbc.connect(constants.URL)
        print("Database connection established.")

def close_db_connection():
    """データベース接続を閉じる"""
    global db_connection
    if db_connection is not None:
        db_connection.close()
        db_connection = None
        print("Database connection closed.")

def get_personal_info():
    """
        個人情報データの取得、IDと名前だけ
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_PERSONAL, db_connection)
    
    return df

def get_keito_wish():
    """
        希望系統（資料請求情報）の取得
        データはA1, A2, A3の3通り
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_KEITO_WISH, db_connection)
    
    # 固定するKEITO_WISHの順番
    all_keito = ['A1', 'A2', 'A3']
    
    result = make_pivot(df, all_keito, '希望系統(資料請求)', 'WISH_KEITO_CD')
    
    return result

def get_keito_jiko():
    """
        希望系統（自己申告）の取得
        データはB1, B2, B3の3通り
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_KEITO_JIKO, db_connection)
    
    # 固定するKEITO_WISHの順番
    all_keito = ['B1', 'B2', 'B3']
    
    result = make_pivot(df, all_keito, '希望系統(自己申告)', 'JIKO_KEITO_CD')
    
    return result
    
def get_seikyu_univ():
    """
        資料請求した大学コードの取得
        カンマ区切りで1列で取得
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_SEIKYU_UNIV, db_connection)
    
    result = df.groupby('PERSONAL_ID')['UNIV_CD'].agg(', '.join).reset_index()
    
    return result

def get_area():
    """
        エリア情報の取得
        データはX, Y, Zの3種類
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_AREA, db_connection)
    
    # 固定するAREAの順番
    all_area = ['X', 'Y', 'Z']

    result = make_pivot(df, all_area, '希望エリア', 'WISH_AREA')
    
    return result

def get_wish_job():
    """
        希望職種の取得
        データは1,2,3の3種類
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_WISH_JOB, db_connection)
    
    all_jobs = ['1', '2', '3']
    
    result = make_pivot(df, all_jobs, '希望職種', 'WISH_JOB_CD')
    
    return result

def get_request_cd():
    """
        リクエストコードの取得
        昇順に最大3個まで
    """
    global db_connection
    df = pd.read_sql(constants.SELECT_REQUEST, db_connection)
    
    # P_IDごとにRES_CDを昇順にソートしてリスト化
    grouped = (
        df.sort_values(by="RESPONSE_CD")  # RES_CDを昇順にソート
        .drop_duplicates(subset=["PERSONAL_ID", "RESPONSE_CD"])  # 重複を削除
        .groupby("PERSONAL_ID")["RESPONSE_CD"]  # P_IDごとにグループ化
        .apply(lambda x: (x.tolist()[:3] + [0] * 3)[:3])  # 各グループで最大3つをリスト化
        .reset_index()  # インデックスをリセット
    )

    # リストを列に展開し、列名を「資料1」「資料2」「資料3」に設定
    result = grouped["RESPONSE_CD"].apply(pd.Series)
    result.columns = ["コード1", "コード2", "コード3"]

    # P_IDを結合
    result.insert(0, "PERSONAL_ID", grouped["PERSONAL_ID"])

    # 結果を表示
    return result
    
def get_all_data():
    try:
        initialize_db_connection()
        
        df1 = get_personal_info()
        df2 = get_keito_wish()
        df3 = get_keito_jiko()
        df4 = get_seikyu_univ()
        df5 = get_area()
        df6 = get_wish_job()
        df7 = get_request_cd()
        
        # データフレームリスト
        dfs = [df1, df2, df3, df4, df5, df6, df7]

        # 1つ目のデータフレームを基に結合を始める
        result = dfs[0]

        # 残りのデータフレームを順番に結合
        for df in dfs[1:]:
            result = pd.merge(result, df, on='PERSONAL_ID', how='inner')
            
        result.to_csv('output.tsv', sep='\t', index=False, encoding='shift-jis', line_terminator='\r\n')
        
    finally:
        close_db_connection()

def get_gaisan():
    """現在のDBデータを取得して配列で返却する
    """
    conn = pyodbc.connect(constants.URL)
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
    
    # df = pd.read_sql(SIRYO_SQL, conn)

    # cursor.close()
    # conn.close()
    
    # print(df)
    
    # df = df.pivot_table(index='P_CD', columns='K_DIC', values='cnt', aggfunc='sum')
    
    # print(df)
    
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
    grouped = df.pivot_table(index=["P_CD", "NOW_GRADE"], 
                        columns="K_DIC", 
                        values="cnt", 
                        aggfunc="sum", 
                        fill_value=0)

    # 固定列を反映し、欠けている列を 0 で補完
    for col in fixed_columns:
        if col not in grouped.columns:
            grouped[col] = 0

    # 列の順序を固定列順に整える
    grouped = grouped[fixed_columns]

    # 各行の合計（新しい列 "Row_Total" を追加）
    # grouped["Row_Total"] = grouped.sum(axis=1)

    # # 各列の合計（新しい行 "Column_Total" を追加）
    # grouped.loc["Column_Total"] = grouped.sum()

    # 結果をリセットインデックスして整形
    # result = grouped.reset_index()

    print(grouped)
    
    return df

def make_pivot(df, fixed_column, column_name_logical, column_name_physical):
    """
        入力されたDataFrameを指定された列に基づいてピボットする関数です。

        この関数は、入力されたDataFrame `df`を、指定された`fixed_column`列を行として固定し、 
        `column_name_logical`列の値を新しい列名として、`column_name_physical`列の値を対応するセルの値としてピボットします。
        ピボット後のDataFrameを返します。

        パラメータ:
        -----------
        df : pandas.DataFrame
            ピボット対象となる入力DataFrame。

        fixed_column : List[str]
            ピボット後の行インデックスとして固定する列名のリスト

        column_name_logical : str
            ピボット後の列名として使われる列名。

        column_name_physical : str
            セルの値として使用される列名。

        戻り値:
        --------
        pandas.DataFrame
            `fixed_column`を行、`column_name_logical`を列、`column_name_physical`をセルの値として
            ピボットされたDataFrame。
    """
    # データに対応する列名を生成
    column_mapping = {column: f"{column_name_logical}{i + 1}" for i, column in enumerate(fixed_column)}

    # ピボットテーブル形式で変換
    result = (
        df.assign(**{col: (df[column_name_physical] == col).astype(int) for col in fixed_column})
        .drop(columns=[column_name_physical])
        .groupby("PERSONAL_ID", as_index=False)
        .max()
    )

    # 列名を動的に変更
    result = result.rename(columns=column_mapping)
    
    return result

if __name__ == '__main__':
    get_gaisan()
    
    data = {
        "PERSONAL_ID": [1, 2, 1, 3],
        "P_CD": [None, None, None, None],
    }
    
    df =pd.DataFrame(data)
    
    print(make_pivot(df, ['A1', 'A2', 'B1'], '請求', 'P_CD'))