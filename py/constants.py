# SQL文
SELECT_PERSONAL = """
    SELECT PERSONAL_ID, NAME FROM PERSONAL
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_KEITO_WISH = """
    SELECT PERSONAL_ID, WISH_KEITO_CD FROM KEITO
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_KEITO_JIKO = """
    SELECT PERSONAL_ID, JIKO_KEITO_CD FROM KEITO
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_SEIKYU_UNIV = """
    SELECT PERSONAL_ID, UNIV_CD FROM REQUEST_UNIV
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_AREA = """
    SELECT PERSONAL_ID, WISH_AREA FROM AREA
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_WISH_JOB = """
    SELECT PERSONAL_ID, WISH_JOB_CD FROM JOB_FIELD
        WHERE PERSONAL_ID IN (
            SELECT PERSONAL_ID FROM REQUEST
                WHERE RESPONSE_CD = 'R001'
        )
    ORDER BY PERSONAL_ID
"""

SELECT_REQUEST = """
    SELECT PERSONAL_ID, RESPONSE_CD FROM REQUEST
    ORDER BY PERSONAL_ID
"""

#DBアクセス
DRIVER = '{SQL Server}'
SERVER = 'HP-ENVY'
DATABASE = 'MANAGE_TEST'
TRUSTED_CONNECTION ='yes'
SQL_SELECT = "SELECT SHC_CD, DEP_CD, CLS_CD, SHC_NAME, DEP_NAME, CLS_NAME, CONCAT(SHC_CD, DEP_CD, CLS_CD) AS 比較用コード FROM SCHOOL_INFO"
    
URL = 'DRIVER='+DRIVER+';SERVER='+SERVER+';DATABASE='+DATABASE+';PORT=1433;Trusted_Connection='+TRUSTED_CONNECTION+';'
