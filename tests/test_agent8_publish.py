import httpx
import pytest

from agents.Agent_8_knowledge_synth.publish import (
    article_to_storage_xml, publish_to_confluence,
)
from agents.Agent_8_knowledge_synth.schemas import ResolutionStep, SynthesizedArticle


def _article():
    return SynthesizedArticle(
        title="DB pool exhausted",
        problem_summary="Pods crash with timeouts.",
        root_cause="Pool too small.",
        resolution_steps=[
            ResolutionStep(step=1, action="Restart", command="kubectl rollout restart deploy/app"),
            ResolutionStep(step=2, action="Raise pool size"),
        ],
        keywords=["HikariCP"], assignment_group="App-Backend",
        category="App", subcategory="DB", confidence_self_rating=0.8,
    )


@pytest.mark.unit
def test_storage_xml_includes_title_summary_and_steps():
    xml = article_to_storage_xml(_article(), source_incident_ids=["INC1", "INC2"])
    assert "DB pool exhausted" in xml
    assert "kubectl rollout restart deploy/app" in xml
    assert "Raise pool size" in xml
    assert "INC1" in xml and "INC2" in xml


@pytest.mark.unit
@pytest.mark.asyncio
async def test_publish_to_confluence_posts_and_returns_page_id():
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.content.decode()
        return httpx.Response(200, json={"id": "9988776655", "title": "[AUTO] DB pool exhausted"})

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, base_url="https://x.atlassian.net/wiki") as client:
        page_id = await publish_to_confluence(
            client=client, space_key="AUTO_KB",
            article=_article(), source_incident_ids=["INC1"],
            auth_token="t",
        )
    assert page_id == "9988776655"
    assert "/api/v2/pages" in captured["url"]
    assert "DB pool exhausted" in captured["body"]
