"""Honest absences: dbt Cloud and SSH tunnels, generated from enforcing modules."""

from __future__ import annotations

from src.ai.first_party.capability_contract import (
    dbt_card,
    dbt_cloud_shipped,
    postgres_connect_accepts_tunnel,
    ssh_tunnel_card,
)
from src.ai.rag.product_docs import compose_product_answer, retrieve_product_answer
from src.ai.rag.product_facts import generated_sections


def test_enforcing_modules_do_not_ship_dbt_cloud_or_ssh_tunnels() -> None:
    assert dbt_cloud_shipped() is False
    assert postgres_connect_accepts_tunnel() is False
    dbt = dbt_card()
    ssh = ssh_tunnel_card()
    assert dbt is not None and "does not run dbt cloud" in dbt.text.lower()
    assert ssh is not None and "does not open ssh tunnels" in ssh.text.lower()
    assert "wal_level" not in dbt.text.lower()
    assert "wal_level" not in ssh.text.lower()


def test_generated_sections_include_the_capability_cards() -> None:
    titles = {section.section_title for section in generated_sections()}
    assert "Does Datawrap run dbt Cloud" in titles
    assert "Does Datawrap open SSH tunnels" in titles


def test_dbt_and_ssh_leads_do_not_steal_cdc_or_studio() -> None:
    dbt = compose_product_answer(
        retrieve_product_answer("can I use dbt with datawrap?", limit=4)
    ) or ""
    ssh = compose_product_answer(
        retrieve_product_answer("do you open an SSH tunnel to postgres?", limit=4)
    ) or ""
    dbt_lead = dbt.split(". ")[0].lower()
    ssh_lead = ssh.split(". ")[0].lower()
    assert "does not run dbt" in dbt_lead
    assert "wal_level" not in dbt_lead
    assert "query playground" not in dbt_lead
    assert "does not open ssh" in ssh_lead
    assert "wal_level" not in ssh_lead
    assert "query playground" not in ssh_lead
