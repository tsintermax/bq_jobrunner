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
from google.cloud import bigquery

class bq_jobrunner :
    
    
    def __init__(self,project_id: str,credentials_path: str,location: str):
       
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
        
    def execute(self):
        from joblib import Parallel, delayed
        while len(self.jobs) != len(self.processed_jobs):
            print(len(self.processed_jobs),"jobs have been processed out of",len(self.jobs),"jobs.")
            self.queue_jobs()
            for job_id in self.queue:
                self.run_job(job_id)
            
        else:
            print("Finished all jobs.")
            
            
    def render_graph(self):
        from graphviz import Digraph
        G = Digraph(format='png')
        G.attr('node', shape='circle')

        for j in self.jobs:
            G.node(str(j), self.jobs[j]['common_name'])
            
        for j in self.jobs:
            for d in self.jobs[j]['dependent_query']:
               G.edge(str(d), str(j))

        G.render(view = True)


##TODO parallel processing

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
        
        
# class MulHelper(object):
#     def __init__(self, cls, mtd_name):
#         self.cls = cls
#         self.mtd_name = mtd_name

#     def __call__(self, *args, **kwargs):
#         return getattr(self.cls, self.mtd_name)(*args, **kwargs)

