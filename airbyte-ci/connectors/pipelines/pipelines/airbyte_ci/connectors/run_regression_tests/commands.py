#
# Copyright (c) 2023 Airbyte, Inc., all rights reserved.
#

import shutil
from typing import Optional

import asyncclick as click
from pipelines import main_logger
from pipelines.airbyte_ci.connectors.context import ConnectorContext
from pipelines.airbyte_ci.connectors.pipeline import run_connectors_pipelines
from pipelines.airbyte_ci.connectors.run_regression_tests.pipeline import run_regression_tests_pipeline
from pipelines.airbyte_ci.connectors.run_regression_tests.steps import RegressionTests
from pipelines.cli.click_decorators import click_ci_requirements_option
from pipelines.cli.dagger_pipeline_command import DaggerPipelineCommand
from pipelines.consts import LOCAL_BUILD_PLATFORM, ContextState
from pipelines.helpers.github import update_global_commit_status_check_for_tests
from pipelines.helpers.utils import fail_if_missing_docker_hub_creds


@click.command(
    cls=DaggerPipelineCommand,
    help="Run regression tests for the selected connectors.",
    context_settings=dict(
        ignore_unknown_options=True,
    ),
)
@click_ci_requirements_option()
@click.option(
    "--connection-id",
    help="The connection ID against which to run tests.",
    default="",
    type=str,
)
@click.option(
    "--control-version",
    help="The control version used for regression testing. Defaults to latest.",
    default="latest",
    type=str,
)
@click.option(
    "--target-version",
    help="The target version used for regression testing. Defaults to dev.",
    default="dev",
    type=str,
)
@click.option(
    "--pr-url",
    help="The URL of the PR you are testing.",
    type=str,
    required=True,
)  # TODO: add options for config/catalog/state path
@click.pass_context
async def test(
    ctx: click.Context, connection_id: Optional[str], control_version: str, target_version: str, pr_url: str
) -> bool:
    """Runs regression tests for the selected connector.

    Args:
        ctx (click.Context): The click context.
    """
    if ctx.obj["is_ci"]:
        fail_if_missing_docker_hub_creds(ctx)

    if ctx.obj["selected_connectors_with_modified_files"]:
        update_global_commit_status_check_for_tests(ctx.obj, "pending")
    else:
        main_logger.warn("No connector were selected for testing.")
        update_global_commit_status_check_for_tests(ctx.obj, "success")
        return True

    connectors_tests_contexts = []
    for connector in ctx.obj["selected_connectors_with_modified_files"]:
        connectors_tests_contexts.append(
            ConnectorContext(
                pipeline_name=f"Testing connector {connector.technical_name}",
                connector=connector,
                is_local=ctx.obj["is_local"],
                git_branch=ctx.obj["git_branch"],
                git_revision=ctx.obj["git_revision"],
                ci_git_user=ctx.obj["ci_git_user"],
                ci_github_access_token=ctx.obj["ci_github_access_token"],
                ci_report_bucket=ctx.obj["ci_report_bucket_name"],
                report_output_prefix=ctx.obj["report_output_prefix"],
                use_remote_secrets=ctx.obj["use_remote_secrets"],
                gha_workflow_run_url=ctx.obj.get("gha_workflow_run_url"),
                dagger_logs_url=ctx.obj.get("dagger_logs_url"),
                pipeline_start_timestamp=ctx.obj.get("pipeline_start_timestamp"),
                ci_context=ctx.obj.get("ci_context"),
                pull_request=ctx.obj.get("pull_request"),
                ci_gcs_credentials=ctx.obj["ci_gcs_credentials"],
                use_local_cdk=ctx.obj.get("use_local_cdk"),
                s3_build_cache_access_key_id=ctx.obj.get("s3_build_cache_access_key_id"),
                s3_build_cache_secret_key=ctx.obj.get("s3_build_cache_secret_key"),
                docker_hub_username=ctx.obj.get("docker_hub_username"),
                docker_hub_password=ctx.obj.get("docker_hub_password"),
                targeted_platforms=[LOCAL_BUILD_PLATFORM],
                regression_test_versions=(control_version, target_version),
                regression_test_connection_id=connection_id,
                regression_test_pr_url=pr_url,
            )
        )

    try:
        await run_connectors_pipelines(
            [connector_context for connector_context in connectors_tests_contexts],
            run_regression_tests_pipeline,
            "Regression Tests Pipeline",
            ctx.obj["concurrency"],
            ctx.obj["dagger_logs_path"],
            ctx.obj["execute_timeout"],
        )
    except Exception as e:
        main_logger.error("An error occurred while running the regression tests pipeline", exc_info=e)
        update_global_commit_status_check_for_tests(ctx.obj, "failure")
        return False
    finally:
        if RegressionTests.regression_tests_artifacts_dir.exists():
            shutil.rmtree(RegressionTests.regression_tests_artifacts_dir)
            main_logger.info(f"🧹 Test artifacts cleaned up from {RegressionTests.regression_tests_artifacts_dir}")

    @ctx.call_on_close
    def send_commit_status_check() -> None:
        if ctx.obj["is_ci"]:
            global_success = all(connector_context.state is ContextState.SUCCESSFUL for connector_context in connectors_tests_contexts)
            update_global_commit_status_check_for_tests(ctx.obj, "success" if global_success else "failure")

    # If we reach this point, it means that all the connectors have been tested so the pipeline did its job and can exit with success.
    return True
