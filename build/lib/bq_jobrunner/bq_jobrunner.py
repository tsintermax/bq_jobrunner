#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
from graphviz import Digraph
from google.cloud import bigquery


class BQJobrunner:
    def __init__(self, project_id: str, credentials_path: str, location: str):
        self.project_id = project_id
        self.location = location
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.client = bigquery.Client()
        self.jobs = {}
        self.processed_jobs = []

    def compose_query(self, query_id: int, sql_str: str, dest_dataset: str,
                      dest_table: str, dependent_query: list =[],
                      common_name=''):
        job_config = bigquery.QueryJobConfig()
        job_config.destination = self.client.dataset(dest_dataset).table(dest_table)
        job_config.create_disposition = 'CREATE_IF_NEEDED'
        job_config.write_disposition = 'WRITE_TRUNCATE'
        job = {
            "query_id": query_id,
            "sql": self.get_query_string(sql_str),
            "job_config": job_config,
            "dependent_query": dependent_query,
            "is_finished": False,
            "common_name": common_name
        }
        self.jobs[query_id] = job

    def queue_jobs(self):
        """Queue all jobs which has no dependency"""
        self.queue = []
        for _, v in self.jobs.items():
            dependent_queries = []
            dependent_queries = set(v['dependent_query']) - set(self.processed_jobs)
            if (v['is_finished'] is False) and (not dependent_queries):
                self.queue.append(v['query_id'])

    def run_job(self, job_id):
        job = self.jobs[job_id]
        print('Running "{0}"(query_id :{1}) ...'.format(
            job["common_name"],
            job["query_id"]
        ))
        query_job = self.client.query(
            job["sql"],
            location=self.location,
            job_config=job["job_config"])
        query_job.result()
        print('Query "{}" has been finished. {:.2f} GBs are processed.'.format(
            job["common_name"], query_job.total_bytes_processed / 1073741824
        ))
        self.jobs[job_id]["is_finished"] = True
        self.processed_jobs.append(job["query_id"])

    def execute(self):
        while len(self.jobs) != len(self.processed_jobs):
            print("{} jobs have been processed out of {} jobs".format(
                len(self.processed_jobs),
                len(self.jobs)
            ))
            self.queue_jobs()
            for job_id in self.queue:
                self.run_job(job_id)
        else:
            print("Finished all jobs.")

    def render_graph(self):
        G = Digraph(name='bq_tables', format='png')
        G.attr('node', shape='circle')

        for j in self.jobs:
            G.node(str(j), self.jobs[j]['common_name'])

        for j in self.jobs:
            for d in self.jobs[j]['dependent_query']:
                G.edge(str(d), str(j))

        G.render(view=True)

    def get_query_string(self, file_path: str) -> str:
        file = open(file_path, 'r')
        bq_file = file.read()
        file.close()
        return bq_file
