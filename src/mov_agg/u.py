import pandas as pd


def merge(load_dt="20240724"):
    read_df = pd.read_parquet('~/tmp/test_parquet')
    cols = ['movieCd', #영화의 대표코드를 출력합니다.
       'movieNm', #영화명(국문)을 출력합니다.
        'openDt', #영화의 개봉일을 출력합니다.
        'audiCnt', #해당일의 관객수를 출력합니다.
        'load_dt', # 입수일자
        'multiMovieYn', #다양성영화 유무
        'repNationCd', #한국외국영화 유무
       ]
    df = read_df[cols]
    # 울버린만 조회 
    #print(df.head(100))
    dw = df[(df['movieCd'] == '20235974') & (df['load_dt'] == int(load_dt))].copy() #날짜조건 load_dt 인자 받기 print(dw)
    print(dw.dtypes)

    # 카테고리 타입 -> Object
    dw['load_dt'] = dw['load_dt'].astype('object')
    dw['multiMovieYn'] = dw['multiMovieYn'].astype('object')
    dw['repNationCd'] = dw['repNationCd'].astype('object')

    # NaN 값 unknown 으로 변경
    dw['multiMovieYn'] = dw['multiMovieYn'].fillna('unknown')
    dw['repNationCd'] = dw['repNationCd'].fillna('unknown')
    print(dw.dtypes)
    print(dw)

    # 머지
    u_mul = dw[dw['multiMovieYn'] == 'unknown']
    u_nat = dw[dw['repNationCd'] == 'unknown']
    m_df = pd.merge(u_mul, u_nat, on='movieCd', suffixes=('_m', '_n'))

    print("머지 DF")
    print(m_df)

    return m_df


merge()

