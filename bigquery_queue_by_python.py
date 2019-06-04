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

# +
class bq_jobrunner :

    def __init__(self,project_id: str,credentials_path: str,location: str):
        from google.cloud import bigquery
        import os
        self.project_id = project_id
        self.location = location
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.client = bigquery.Client()
        self.jobs = {}
        self.processed_jobs =[]
        
    def compose_query(self,query_id :int, sql: str, dest_dataset:str,
                      dest_table:str,dependent_query: list =[],common_name=''):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.client.dataset(dest_dataset).table(dest_table)
        job_config.create_disposition = 'CREATE_IF_NEEDED'
        job_config.write_disposition = 'WRITE_TRUNCATE'
        
        job = {
            "query_id"         : query_id
            ,"sql"             : sql
            ,"job_config"      : job_config
            ,"dependent_query" : dependent_query
            ,"is_finished"     : False
            ,"common_name"     : common_name
        }
        
        self.jobs[query_id] = job

        
        
    def queue_jobs(self):
        self.queue = []
        #queue all jobs which has no dependency
        for k,v in self.jobs.items():
            dependent_queries = []
            dependent_queries = set(v['dependent_query']) - set(self.processed_jobs)
            if (v['is_finished'] == False) and (not dependent_queries):
                self.queue.append(v['query_id'])

    
    def run_job(self,job_id):
        job = self.jobs[job_id]
        print('Running "{0}"(query_id :{1}) ...'.format(job["common_name"],job["query_id"]))
        query_job = self.client.query(
            job["sql"],
            location=self.location,
            job_config=job["job_config"])
        query_job.result()
        print('Query "{}" has been finished. {:.2f} GBs are processed.'
                .format(job["common_name"],query_job.total_bytes_processed/1073741824))
        self.jobs[job_id]["is_finished"] = True
        self.processed_jobs.append(job["query_id"])
        
    
#     def execute(self):
#         from joblib import Parallel, delayed
#         while len(self.jobs) != len(self.processed_jobs):
#             print(len(self.processed_jobs),"jobs have been processed out of",len(self.jobs),"jobs.")
#             self.queue_jobs()
#             import copy
            
#             q = copy.deepcopy(self.queue)
#             Parallel(n_jobs=2, verbose=10)([delayed(MulHelper(self, 'run_job'))(job_id) for job_id in q])
            
#         else:
#             print("Finished all jobs.")

    def execute(self):
        from joblib import Parallel, delayed
        while len(self.jobs) != len(self.processed_jobs):
            print(len(self.processed_jobs),"jobs have been processed out of",len(self.jobs),"jobs.")
            self.queue_jobs()
            for job_id in self.queue:
                self.run_job(job_id)
            
        else:
            print("Finished all jobs.")

            
class MulHelper(object):
    def __init__(self, cls, mtd_name):
        self.cls = cls
        self.mtd_name = mtd_name

    def __call__(self, *args, **kwargs):
        return getattr(self.cls, self.mtd_name)(*args, **kwargs)


# -


a = bq_jobrunner('sugisaki-sandbox','/Users/takuto/.credentials.json','asia-northeast1')


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

a.execute()


