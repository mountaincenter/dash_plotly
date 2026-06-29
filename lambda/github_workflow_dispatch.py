import json
import os
import urllib.error
import urllib.request
from typing import Any

import boto3


SSM_CLIENT = boto3.client("ssm")


def _get_required_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value


def _get_github_token(parameter_name: str) -> str:
    response = SSM_CLIENT.get_parameter(Name=parameter_name, WithDecryption=True)
    token = response["Parameter"]["Value"]
    if not token:
        raise RuntimeError("GitHub token parameter is empty")
    return token


def _normalize_inputs(value: Any) -> dict[str, str]:
    if value in (None, ""):
        return {}
    if not isinstance(value, dict):
        raise RuntimeError("event.inputs must be an object when provided")

    inputs: dict[str, str] = {}
    for key, item in value.items():
        if isinstance(item, bool):
            inputs[str(key)] = "true" if item else "false"
        elif item is None:
            inputs[str(key)] = ""
        else:
            inputs[str(key)] = str(item)
    return inputs


def _dispatch_workflow(
    *,
    token: str,
    owner: str,
    repo: str,
    workflow_id: str,
    api_version: str,
    payload: dict[str, Any],
) -> int:
    url = f"https://api.github.com/repos/{owner}/{repo}/actions/workflows/{workflow_id}/dispatches"
    request = urllib.request.Request(
        url,
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json",
            "User-Agent": "dash-plotly-eventbridge-scheduler",
            "X-GitHub-Api-Version": api_version,
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            response_body = response.read().decode("utf-8")
            print(
                "GitHub workflow_dispatch response: "
                f"status={response.status}, body_length={len(response_body)}"
            )
            return response.status
    except urllib.error.HTTPError as error:
        error_body = error.read().decode("utf-8", errors="replace")
        print(
            "GitHub workflow_dispatch failed: "
            f"status={error.code}, body={error_body[:1000]}"
        )
        raise


def lambda_handler(event: dict[str, Any], context: Any) -> dict[str, Any]:
    owner = os.environ.get("GITHUB_OWNER", "mountaincenter")
    repo = os.environ.get("GITHUB_REPO", "dash_plotly")
    workflow_id = os.environ.get("GITHUB_WORKFLOW_ID", "data-pipeline.yml")
    default_ref = os.environ.get("GITHUB_REF", "main")
    api_version = os.environ.get("GITHUB_API_VERSION", "2022-11-28")
    parameter_name = _get_required_env("GITHUB_TOKEN_PARAMETER_NAME")

    ref = str(event.get("ref") or default_ref)
    inputs = _normalize_inputs(event.get("inputs"))
    mode = str(event.get("mode") or "").strip()
    if mode:
        existing_mode = inputs.get("mode", "").strip()
        if existing_mode and existing_mode != mode:
            raise RuntimeError(
                f"Conflicting dispatch modes: event.mode={mode}, inputs.mode={existing_mode}"
            )
        inputs["mode"] = mode

    payload: dict[str, Any] = {"ref": ref}
    if inputs:
        payload["inputs"] = inputs

    print(
        "Dispatching GitHub workflow: "
        f"repository={owner}/{repo}, workflow_id={workflow_id}, ref={ref}, "
        f"mode={mode or 'unspecified'}, input_keys={sorted(inputs.keys())}"
    )

    token = _get_github_token(parameter_name)
    status = _dispatch_workflow(
        token=token,
        owner=owner,
        repo=repo,
        workflow_id=workflow_id,
        api_version=api_version,
        payload=payload,
    )

    if status != 204:
        raise RuntimeError(f"Unexpected GitHub workflow_dispatch status: {status}")

    return {
        "statusCode": 204,
        "body": json.dumps(
            {
                "repository": f"{owner}/{repo}",
                "workflow_id": workflow_id,
                "ref": ref,
                "mode": mode or "unspecified",
            }
        ),
    }
