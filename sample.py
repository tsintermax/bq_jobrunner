#!/usr/bin/env python
# -*- coding: utf-8 -*-
from bq_jobrunner.bq_jobrunner import BQJobrunner


project_id = "project_id"
credential_path = "~/config/.credentials.json"
location = "asia-northeast1"
bq_jobs = BQJobrunner(project_id, location=location)

bq_jobs.compose_query(
    query_id=1,
    sql_str="./queries/sql01.bq",
    dest_dataset="",
    dest_table="",
    common_name="",
)

bq_jobs.compose_query(
    query_id=2,
    sql_str="./queries/sql02.bq",
    dest_dataset="",
    dest_table="",
    dependent_query=[1],
    common_name="",
)

bq_jobs.execute(run_queries=False, export_json=True, render_graph=True)
