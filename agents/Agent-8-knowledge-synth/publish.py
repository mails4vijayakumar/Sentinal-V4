from __future__ import annotations

import html
import logging
from typing import Optional

import httpx

from agents.Agent_8_knowledge_synth.schemas import SynthesizedArticle

logger = logging.getLogger("agent8.publish")


def article_to_storage_xml(article: SynthesizedArticle, source_incident_ids: list[str]) -> str:
    steps_xml = "".join(
        f"<li>{html.escape(s.action)}"
        + (f"<br/><code>{html.escape(s.command)}</code>" if s.command else "")
        + "</li>"
        for s in article.resolution_steps
    )
    incidents_xml = ", ".join(html.escape(i) for i in source_incident_ids)
    root_cause_xml = (
        f"<h2>Root cause</h2><p>{html.escape(article.root_cause)}</p>"
        if article.root_cause else ""
    )
    keywords_xml = ", ".join(html.escape(k) for k in article.keywords)
    return (
        f"<h1>{html.escape(article.title)}</h1>"
        f"<h2>Problem summary</h2><p>{html.escape(article.problem_summary)}</p>"
        f"{root_cause_xml}"
        f"<h2>Resolution steps</h2><ol>{steps_xml}</ol>"
        f"<h2>Keywords</h2><p>{keywords_xml}</p>"
        f"<hr/><p><em>Auto-synthesized from {len(source_incident_ids)} incidents: {incidents_xml}</em></p>"
    )


async def publish_to_confluence(
    *,
    client: httpx.AsyncClient,
    space_key: str,
    article: SynthesizedArticle,
    source_incident_ids: list[str],
    auth_token: str,
    parent_page_id: Optional[str] = None,
) -> str:
    """POST a new page in storage format. Returns the Confluence page_id."""
    body_xml = article_to_storage_xml(article, source_incident_ids)
    payload = {
        "spaceId": space_key,  # caller is responsible for passing spaceId, not key, when v2 requires
        "status": "current",
        "title": f"[AUTO] {article.title}",
        "body": {"representation": "storage", "value": body_xml},
    }
    if parent_page_id:
        payload["parentId"] = parent_page_id

    resp = await client.post(
        "/api/v2/pages",
        json=payload,
        headers={"Authorization": f"Bearer {auth_token}", "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()["id"]
