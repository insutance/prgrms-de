import psycopg2

def get_Redshift_connection():
    host = "learnde.cduaw970ssvt.ap-northeast-2.redshift.amazonaws.com"
    redshift_user = "insutance"
    redshift_pass = "Insutance!1"
    port = 5439
    dbname = "prod"
    conn = psycopg2.connect("dbname={dbname} user={user} host={host} password={password} port={port}".format(
        dbname=dbname,
        user=redshift_user,
        password=redshift_pass,
        host=host,
        port=port
    ))
    conn.set_session(readonly=True, autocommit=True)
    return conn.cursor()

cur = get_Redshift_connection()

sql = """
        select to_char(ts, 'YYYY-MM') as month , count(distinct userid)
        from raw_data.user_session_channel as sc
        join raw_data.session_timestamp as st
        on sc.sessionid = st.sessionid
        group by month;
    """
cur.execute(sql)
rows = cur.fetchall()

for r in rows:
    print(r)