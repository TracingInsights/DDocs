from pathlib import Path

import yaml

WORKFLOW_PATH = Path(__file__).parents[1] / ".github" / "workflows" / "scrape.yml"


def load_workflow() -> dict[str, object]:
    """Load workflow scalars as strings so GitHub's `on` key is not treated as YAML 1.1 boolean syntax."""
    return yaml.load(WORKFLOW_PATH.read_text(), Loader=yaml.BaseLoader)


def test_scrape_workflow_has_only_manual_and_daily_fallback_triggers() -> None:
    triggers = load_workflow()["on"]

    assert set(triggers) == {"schedule", "workflow_dispatch"}
    assert triggers["workflow_dispatch"] == ""
    assert triggers["schedule"] == [{"cron": "17 6 * * *"}]


def test_daily_fallback_runs_at_exactly_0617_utc() -> None:
    cron = load_workflow()["on"]["schedule"][0]["cron"]
    minute, hour, day_of_month, month, day_of_week = cron.split()

    assert (minute, hour) == ("17", "6")
    assert (day_of_month, month, day_of_week) == ("*", "*", "*")


def test_concurrent_scrapes_queue_without_cancelling_active_run() -> None:
    workflow = load_workflow()

    assert workflow["concurrency"] == {
        "group": "fia-scrape",
        "cancel-in-progress": "false",
    }
    assert all("concurrency" not in job for job in workflow["jobs"].values())
