#!/usr/bin/env python
# -*- coding: utf-8 -*-
import os
import json
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from graphviz import Digraph
from google.cloud import bigquery
from networkx.drawing.nx_pydot import read_dot
from networkx.algorithms.cycles import find_cycle
from networkx import NetworkXNoCycle


class BQJobrunner:
    def __init__(
        self,
        project_id: str,
        credentials_path: str = "",
        location: str = "asia-northeast1",
        replace_strings_dict={},
    ):
        self.project_id = project_id
        self.location = location
        if credentials_path:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
        self.client = bigquery.Client()
        self.jobs = {}
        self.processed_jobs = []
        self.to_json = {}
        self.__replace_strings_dict = replace_strings_dict
        self.running_jobs = set()
        self.lock = threading.Lock()

    def compose_query(
        self,
        query_id: int,
        sql_str: str,
        dest_dataset: str,
        dest_table: str,
        dependent_query: list = [],
        common_name="",
    ):
        job_config = bigquery.QueryJobConfig()
        job_config.create_disposition = "CREATE_IF_NEEDED"
        job_config.destination = self.client.dataset(dest_dataset).table(dest_table)
        job_config.write_disposition = "WRITE_TRUNCATE"
        job = {
            "query_id": query_id,
            "sql": self.__get_query_string(sql_str),
            "job_config": job_config,
            "dependent_query": dependent_query,
            "is_finished": False,
            "common_name": common_name,
        }
        self.jobs[query_id] = job

    def compose_query_by_digraph(self, graph):
        if self.jobs:
            raise ValueError("jobs are not empty")
        try:
            if find_cycle(graph):
                raise ValueError("cycle found in graph, not a dag")
        except NetworkXNoCycle:
            pass
        for node in graph.nodes():
            query_id = int(node)
            path = eval(graph.nodes[node]["label"])
            job = {
                "query_id": query_id,
                "sql": self.__get_query_string(path),
                "job_config": bigquery.QueryJobConfig(),
                "dependent_query": list(
                    map(int, [edge[0] for edge in graph.in_edges(node)])
                ),
                "is_finished": False,
                "common_name": path,
            }
            self.jobs[query_id] = job

    def compose_query_by_dot_path(self, dot_path: str):
        g = read_dot(dot_path)
        self.compose_query_by_digraph(g)

    def queue_jobs(self):
        """Return runnable jobs"""
        count = 0
        next_jobs = []
        for _, v in self.jobs.items():
            dependent_queries = set(v["dependent_query"]) - set(self.processed_jobs)
            if (
                (v["is_finished"] is False)
                and (not dependent_queries)
                and (v["query_id"] not in self.running_jobs)
            ):
                self.running_jobs.add(v["query_id"])
                next_jobs.append(v["query_id"])
                count += 1
        if count:
            print(f"queued {count} jobs!")
        return next_jobs

    def run_job(self, job_id):
        job = self.jobs[job_id]
        print(
            'Running "{0}"(query_id :{1}) ...'.format(
                job["common_name"], job["query_id"]
            )
        )
        query_job = self.client.query(
            job["sql"], location=self.location, job_config=job["job_config"]
        )
        query_job.result()
        print(
            'Query "{}" has been finished. {:.2f} GBs are processed and {:.2f} GBs are billed.'.format(
                job["common_name"],
                query_job.total_bytes_processed / 1073741824,
                query_job.total_bytes_billed / 1073741824,
            )
        )

        if query_job.cache_hit:
            print("Cache hit!")

        if job["job_config"].destination:
            dest_table = bigquery.Table()
            table = self.client.get_table(dest_table)
            self.to_json[str(job_id)] = {
                "from": [
                    self.__table_ref_to_string(table_ref)
                    for table_ref in query_job.referenced_tables
                ],
                "to": self.__table_ref_to_string(table.reference),
                "table_info": {
                    "created": table.created.strftime("%m/%d/%Y %H:%M:%S"),
                    "modified": table.modified.strftime("%m/%d/%Y %H:%M:%S"),
                    "description": table.description,
                    "num_bytes": table.num_bytes,
                    "num_rows": table.num_rows,
                },
            }

        with self.lock:
            self.jobs[job_id]["is_finished"] = True
            self.processed_jobs.append(job["query_id"])
            self.running_jobs.remove(job_id)
            return self.queue_jobs()

    def execute(
        self,
        run_queries: bool = True,
        export_json: bool = True,
        render_graph: bool = False,
    ):
        if render_graph:
            self.__render_graph()

        if run_queries:
            futures = set()
            with ThreadPoolExecutor() as pool:
                for job_id in self.queue_jobs():
                    futures.add(pool.submit(self.run_job, job_id))
                while futures:
                    for future in as_completed(futures):
                        futures.remove(future)
                        for job_id in future.result():
                            futures.add(pool.submit(self.run_job, job_id))

            print("Finished all jobs.")

            if export_json:
                self.__export_json()

    def __render_graph(self):
        G = Digraph(name="bq_tables", format="png")
        G.attr("node", shape="circle")

        for j in self.jobs:
            G.node(str(j), self.jobs[j]["common_name"])

        for j in self.jobs:
            for d in self.jobs[j]["dependent_query"]:
                G.edge(str(d), str(j))

        G.render(view=True)

    def __get_query_string(self, file_path: str) -> str:
        with open(file_path, "r") as f:
            query_string = f.read()
            # Sort key to prevent replacing substrings.
            for key, value in sorted(
                self.__replace_strings_dict.items(), key=lambda x: x[0], reverse=True
            ):
                query_string = query_string.replace(key, value)
            return query_string

    def __table_ref_to_string(self, table_ref):
        project = table_ref.project
        dataset_id = table_ref.dataset_id
        table_id = table_ref.table_id
        return "{}.{}.{}".format(project, dataset_id, table_id)

    def __export_json(self):
        with open("table_dependencies.json", "w") as f:
            json.dump(self.to_json, f)
