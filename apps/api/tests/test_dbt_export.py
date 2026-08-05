"""dbt export hook — complement warehouse ELT, not a dbt Cloud product."""

from services.dbt_export import export_dbt_files
from services.transform_models import DataTest, TransformModel
from services.transform_store import TransformProject


def test_export_dbt_includes_sources_and_models():
    project = TransformProject(
        name="Revenue Rollup",
        destination_connector_id="conn-1",
        schema="analytics",
        models=[
            TransformModel(
                name="daily_revenue",
                sql=(
                    "SELECT order_id, amount FROM {{ source('orders') }} "
                    "WHERE amount > 0"
                ),
                materialization="view",
                description="Daily revenue from landed orders",
                tests=[
                    DataTest(test_type="not_null", column="order_id"),
                    DataTest(test_type="unique", column="order_id"),
                ],
                tags=["finance"],
            )
        ],
    )
    pack = export_dbt_files(project)
    assert pack["honesty"]["is_dbt_cloud"] is False
    assert pack["file_count"] >= 4
    files = pack["files"]
    assert "dbt_project.yml" in files
    assert "models/sources.yml" in files
    assert "orders" in files["models/sources.yml"]
    assert "models/daily_revenue.sql" in files
    assert "config(materialized=" in files["models/daily_revenue.sql"]
    assert "not_null" in files["models/schema.yml"]
    assert "Complement" in files["README_DATAWRAP.md"] or "complement" in files["README_DATAWRAP.md"].lower()


def test_export_dbt_empty_sources_is_honest():
    project = TransformProject(
        name="No Sources",
        destination_connector_id="conn-1",
        models=[
            TransformModel(
                name="constants",
                sql="SELECT 1 AS one",
                materialization="view",
            )
        ],
    )
    pack = export_dbt_files(project)
    assert "sources: []" in pack["files"]["models/sources.yml"]
