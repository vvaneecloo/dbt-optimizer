import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import List, Optional, Dict


@dataclass
class Timing:
    name: str
    started_at: datetime
    completed_at: datetime

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            name=data["name"],
            started_at=datetime.fromisoformat(
                data["started_at"].replace("Z", "+00:00")
            ),
            completed_at=datetime.fromisoformat(
                data["completed_at"].replace("Z", "+00:00")
            ),
        )


@dataclass
class AdapterResponse:
    _message: str
    query_id: str


@dataclass
class Result:
    status: str
    timing: List[Timing]
    thread_id: str
    execution_time: float
    adapter_response: AdapterResponse
    message: str
    failures: Optional[List[str]]
    unique_id: str
    compiled: bool
    compiled_code: str
    relation_name: str

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            status=data["status"],
            timing=[Timing.from_dict(t) for t in data["timing"]],
            thread_id=data["thread_id"],
            execution_time=data["execution_time"],
            adapter_response=AdapterResponse(**data["adapter_response"]),
            message=data["message"],
            failures=data.get("failures"),
            unique_id=data["unique_id"],
            compiled=data["compiled"],
            compiled_code=data["compiled_code"],
            relation_name=data["relation_name"],
        )


@dataclass
class Metadata:
    dbt_schema_version: str
    dbt_version: str
    generated_at: datetime
    invocation_id: str
    env: Dict[str, str]

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            dbt_schema_version=data["dbt_schema_version"],
            dbt_version=data["dbt_version"],
            generated_at=datetime.fromisoformat(
                data["generated_at"].replace("Z", "+00:00")
            ),
            invocation_id=data["invocation_id"],
            env=data["env"],
        )


@dataclass
class Args:
    partial_parse_file_diff: bool
    warn_error_options: Dict[str, List[str]]
    log_level_file: str
    macro_debugging: bool
    log_path: str
    print: bool
    project_dir: str
    favor_state: bool
    log_format_file: str
    source_freshness_run_project_hooks: bool
    target: str
    empty: bool
    log_file_max_bytes: int
    send_anonymous_usage_stats: bool
    select: List[str]
    indirect_selection: str
    require_resource_names_without_spaces: bool
    printer_width: int
    strict_mode: bool
    enable_legacy_logger: bool
    partial_parse: bool
    defer: bool
    which: str
    version_check: bool
    require_explicit_package_overrides_for_builtin_materializations: bool
    use_colors: bool
    cache_selected_only: bool
    introspect: bool
    invocation_command: str
    populate_cache: bool
    exclude: List[str]
    show_resource_report: bool
    vars: Dict
    write_json: bool
    log_format: str
    use_colors_file: bool
    log_level: str
    quiet: bool
    static_parser: bool
    profiles_dir: str


@dataclass
class RunResults:
    metadata: Metadata
    results: List[Result]
    elapsed_time: float
    args: Args

    @classmethod
    def from_dict(cls, data: dict):
        return cls(
            metadata=Metadata.from_dict(data["metadata"]),
            results=[Result.from_dict(r) for r in data["results"]],
            elapsed_time=data["elapsed_time"],
            args=Args(**data["args"]),
        )

    @classmethod
    def parse_json(cls, file_path: str) -> "RunResults":
        """Parse the JSON file and return a RunResults object."""
        with open(file_path, "r") as f:
            data = json.load(f)
        return cls.from_dict(data)

    @classmethod
    def get_compiled_sql(self, path="./target/run_results.json"):
        parsed_data = RunResults.parse_json(file_path=path)
        for result in parsed_data.results:
            compiled = result.compiled_code
        return compiled
