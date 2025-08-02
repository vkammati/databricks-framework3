"""
This script creates/updates all Databricks resources necessary for the pipeline to run properly:
- the DLT pipeline itself
- the workflow that wraps the DLT pipeline
- the workflow that registers tables to Unity Catalog
- the (interactive) cluster used to register tables to Unity Catalog

For each resource, the process is the same:
- get the name of the resource
- get the configuration template
- render the configuration template
- cast the rendered configuration as a dict
- call the Databricks REST API to create or update the resource
"""
#Added comment to test code

import argparse
from jinja2 import Environment, FileSystemLoader
import json
import os
import sys

from ci_cd_helpers.auth import get_dbx_http_header
from ci_cd_helpers.azure import generate_spn_ad_token
from ci_cd_helpers.helpers import (
    get_cluster_name,
    get_dlt_pipeline_name,
    get_register_table_uc_workflow_name,
    get_repos_parent_folder_path,
    get_repos_path,
    get_unique_repo_branch_id,
    get_wrapping_workflow_name
)
from ci_cd_helpers.clusters import create_or_update_cluster_by_name, get_cluster_id
from ci_cd_helpers.pipelines import create_or_update_pipeline_by_name
from ci_cd_helpers.workflows import create_or_update_workflow_by_name
from ci_cd_helpers.workspace import create_folder_if_not_exists


parser = argparse.ArgumentParser(description="Create all Databricks entites for the pipeline.")

parser.add_argument(
    "--host",
    help="Databricks host.",
    type=str,
)

parser.add_argument(
    "--tenant-id",
    help="Azure tenant id.",
    type=str,
)

parser.add_argument(
    "--spn-client-id",
    help="Service principal client id used to deploy code.",
    type=str,
)

parser.add_argument(
    "--spn-client-secret",
    help="Service principal client secret used to deploy code.",
    type=str,
)

parser.add_argument(
    "--repository",
    help="GitHub repository (with owner).",
    type=str,
)

parser.add_argument(
    "--branch-name",
    help="Name of the branch in GitHub",
    type=str,
)

parser.add_argument(
    "--emails",
    help="List of emails, separated by blank spaces, to contact when jobs fail.",
    type=str,
    nargs="*",
    default=[],
)

parser.add_argument(
    "--timeout",
    help="Timeout in seconds for the wrapping workflow. Default is 0, i.e. no timeout.",
    type=int,
    default=0,
)

parser.add_argument(
    "--env",
    help="Databricks/GitHub environment to use.",
    choices=["dev", "tst", "pre", "prd"],
    type=str,
)

parser.add_argument(
    "--deployment-type",
    help="Deployment type, either 'manual' or 'automatic'. Taken into account only when env=dev.",
    choices=["manual", "automatic"],
    type=str,
)

parser.add_argument(
    "--integration-test",
    help="Integration test type, either 'true' or 'false'. Taken into account only when env=dev.",
    choices=["true", "false"],
    type=str,
)

args = parser.parse_args()

#if args.env == "dev" and not args.deployment_type:
#    raise ValueError("When env=dev, parameter 'type' must be either 'manual' or 'automatic', not None.")


dlt_pipeline_conf_filename = "dlt_pipeline_conf.json"
workflow_conf_filename = "workflow_conf.json"
dv_workflow_conf_filename = "dv_workflow_conf.json"
uom_workflow_conf_filename = "uom_workflow_conf.json"
uom_demo_workflow_conf_filename = "uom_demo_workflow_conf.json"
register_table_uc_conf_filename = "register_table_uc_conf.json"
cluster_conf_filename = "cluster_conf.json"

if args.integration_test == "true":
    
    unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, args.deployment_type )
    # Data validation
    dv_suffix = "DV-" +args.deployment_type
    uom_suffix = "UOM-" +args.deployment_type
    uom_demo_suffix = "UOM-DEMO-" +args.deployment_type
    dv_unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, dv_suffix)
    uom_unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, uom_suffix)
    uom_demo_unique_repo_branch_id = get_unique_repo_branch_id(args.repository, args.branch_name, uom_demo_suffix)
else:
    unique_repo_branch_id = ""
    dv_unique_repo_branch_id = ""
    uom_unique_repo_branch_id = ""
    uom_demo_unique_repo_branch_id = ""
    dv_suffix = "-DV"
    uom_suffix = "-UOM"
    uom_demo_suffix = "-UOM-DEMO"

parent_folder_path = get_repos_parent_folder_path(args.repository)

if args.integration_test == "true":
    repos_path = get_repos_path(args.repository, args.branch_name, args.deployment_type)
else:
    repos_path = parent_folder_path + "/datta_tech_eh"
requirements_filepath = f"{repos_path}/requirements.txt"

dir_path = os.path.dirname(os.path.abspath(__file__))
conf_folder_path = f"{dir_path}/../../conf"
common_conf_folder_path = f"{conf_folder_path}/common"

# the DLT and wrapping workflow confs are either in "integration_tests" or in "common" based on the environment we are using
#pipeline_conf_folder = "integration_tests" if args.env == "dev" else "common"
# pipeline_conf_folder = "common"
pipeline_conf_folder = "integration_tests" if args.integration_test == "true" else "common"
pipeline_conf_folder_path = f"{conf_folder_path}/{pipeline_conf_folder}"

environment_conf_folder = "integration_tests" if args.integration_test == "true" else args.env
environment_conf_folder_path = f"{conf_folder_path}/{environment_conf_folder}"

# necessary to call the Databricks REST API
ad_token = generate_spn_ad_token(args.tenant_id, args.spn_client_id, args.spn_client_secret)
http_header = get_dbx_http_header(ad_token)

# folders where Jinja templates are stored
common_templates = Environment(loader=FileSystemLoader(common_conf_folder_path))
pipeline_templates = Environment(loader=FileSystemLoader(pipeline_conf_folder_path))
environment_templates = Environment(loader=FileSystemLoader(environment_conf_folder_path))

# create or update cluster
cluster_conf_template = common_templates.get_template(cluster_conf_filename)

if args.integration_test == "true":
    cluster_name = get_cluster_name(args.repository, args.branch_name, args.deployment_type)
    cluster_conf_str = cluster_conf_template.render(cluster_name=cluster_name)
    cluster_conf = json.loads(cluster_conf_str)
    cluster_id = create_or_update_cluster_by_name(cluster_name, cluster_conf, args.host, http_header)
else:
    cluster_id=None
    #cluster_name = "sede-x-datta-fcb-cluster"
    # cluster_name = get_cluster_name(args.repository)
# cluster_conf_str = cluster_conf_template.render(cluster_name=cluster_name)
# cluster_conf = json.loads(cluster_conf_str)
# cluster_id = create_or_update_cluster_by_name(cluster_name, cluster_conf, args.host, http_header)

# create or update register_table_uc workflow
# register_table_uc_conf_template = common_templates.get_template(register_table_uc_conf_filename)

# if args.integration_test == "true":
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #register_table_uc_name = "sede-x-datta-fcb-uc"
#     register_table_uc_name = get_register_table_uc_workflow_name(args.repository)

# register_table_uc_conf_str = register_table_uc_conf_template.render(
#     job_name=register_table_uc_name,
#     repos_path=repos_path,
#     cluster_id=cluster_id,
#     emails=args.emails,
# )

# register_table_uc_conf = json.loads(register_table_uc_conf_str)
# register_uc_job_id = create_or_update_workflow_by_name(register_table_uc_name, register_table_uc_conf, args.host, http_header)

# get DLT pipeline conf
# dlt_conf_template = pipeline_templates.get_template(dlt_pipeline_conf_filename)

# if args.integration_test == "true":
#     dlt_pipeline_name = get_dlt_pipeline_name(args.repository, args.branch_name, args.deployment_type)
# else:
#     #dlt_pipeline_name = "sede-x-datta-fcb-dlt"
#     dlt_pipeline_name = get_dlt_pipeline_name(args.repository)
# is_dlt_development = args.env == "dev" and args.deployment_type == "manual"

# dlt_pipeline_conf_str = dlt_conf_template.render(
#     dlt_pipeline_name=dlt_pipeline_name,
#     requirements_filepath=requirements_filepath,
#     #register_uc_job_id=register_uc_job_id,
#     repos_path=repos_path,
#     host=args.host,
#     env=args.env,
#     unique_repo_branch_id=unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
#     development=is_dlt_development,
# )

# dlt_pipeline_conf = json.loads(dlt_pipeline_conf_str)
# pipeline_id = create_or_update_pipeline_by_name(dlt_pipeline_name, dlt_pipeline_conf, args.host, http_header)

# create or update wrapping workflow
workflow_conf_template = environment_templates.get_template(workflow_conf_filename)

if args.integration_test == "true":

    workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, args.deployment_type)
else:    
    workflow_name = get_wrapping_workflow_name(args.repository)

workflow_conf_str = workflow_conf_template.render(
    workflow_name=workflow_name,
    #dlt_pipeline_id=pipeline_id,
    unique_repo_branch_id=unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    #register_table_to_uc_workflow_id=register_uc_job_id,
    cluster_id=cluster_id,
    requirements_filepath=requirements_filepath,
)

workflow_conf = json.loads(workflow_conf_str)
create_or_update_workflow_by_name(workflow_name, workflow_conf, args.host, http_header)

# create or update dv wrapping workflow
dv_workflow_conf_template = environment_templates.get_template(dv_workflow_conf_filename)

if args.integration_test == "true":
    dv_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, dv_suffix)
else:    
    dv_workflow_full_suffix = args.repository+dv_suffix    
    dv_workflow_name = get_wrapping_workflow_name(dv_workflow_full_suffix)

dv_workflow_conf_str = dv_workflow_conf_template.render(
    dv_workflow_name=dv_workflow_name,
    unique_repo_branch_id=dv_unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
    requirements_filepath=requirements_filepath,
)

dv_workflow_conf = json.loads(dv_workflow_conf_str)
create_or_update_workflow_by_name(dv_workflow_name, dv_workflow_conf, args.host, http_header)


# create or update uom wrapping workflow
uom_workflow_conf_template = environment_templates.get_template(uom_workflow_conf_filename)

if args.integration_test == "true":
    uom_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, uom_suffix)
else:    
    uom_workflow_full_suffix = args.repository+uom_suffix    
    uom_workflow_name = get_wrapping_workflow_name(uom_workflow_full_suffix)

uom_workflow_conf_str = uom_workflow_conf_template.render(
    uom_workflow_name=uom_workflow_name,
    unique_repo_branch_id=uom_unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
    requirements_filepath=requirements_filepath,
)

uom_workflow_conf = json.loads(uom_workflow_conf_str)
create_or_update_workflow_by_name(uom_workflow_name, uom_workflow_conf, args.host, http_header)

# create or update uom demo wrapping workflow
uom_demo_workflow_conf_template = environment_templates.get_template(uom_demo_workflow_conf_filename)

if args.integration_test == "true":
    uom_demo_workflow_name = get_wrapping_workflow_name(args.repository, args.branch_name, uom_demo_suffix)
else:    
    uom_demo_workflow_full_suffix = args.repository+uom_demo_suffix    
    uom_demo_workflow_name = get_wrapping_workflow_name(uom_demo_workflow_full_suffix)

uom_demo_workflow_conf_str = uom_demo_workflow_conf_template.render(
    uom_demo_workflow_name=uom_demo_workflow_name,
    unique_repo_branch_id=uom_demo_unique_repo_branch_id,  # this will not be taken into account in tst/pre/prd
    emails=args.emails,
    timeout=args.timeout,
    repos_path=repos_path,
    env=args.env,
    host=args.host,
    cluster_id=cluster_id,
    requirements_filepath=requirements_filepath,
)

uom_demo_workflow_conf = json.loads(uom_demo_workflow_conf_str)
create_or_update_workflow_by_name(uom_demo_workflow_name, uom_demo_workflow_conf, args.host, http_header)
