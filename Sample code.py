# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.3'
#       jupytext_version: 1.0.5
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import bq_jobrunner as jr

a = jr.bq_jobrunner('sugisaki-sandbox','/Users/takuto/.credentials.json','asia-northeast1')

sql = '''

SELECT * FROM `sugisaki-sandbox.bq_test.TR_DELIVERY_001` 
where date between '2016-01-01' and '2017-01-01'

'''
a.compose_query(1,sql,"bq_test","query_1",common_name='2016_del')

sql2 = '''

SELECT * FROM `sugisaki-sandbox.bq_test.TR_DELIVERY_000` 
where date not between '2015-01-01' and '2016-01-01'

'''
a.compose_query(2,sql2,"bq_test","query_2",common_name='2015_del')

sql3 = '''

SELECT * FROM `sugisaki-sandbox.bq_test.query_1`

union all 

SELECT * FROM `sugisaki-sandbox.bq_test.query_2`

'''
a.compose_query(3,sql3,"bq_test","query_3",common_name='union',dependent_query=[1,2])

sql4 = '''

SELECT data_kbn, count(data_kbn) as cnt FROM `sugisaki-sandbox.bq_test.query_3`
group by data_kbn

'''
a.compose_query(4,sql4,"bq_test","query_4",common_name='group by',dependent_query=[3])

a.render_graph()

a.execute()


