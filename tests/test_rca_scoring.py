"""tests/test_rca_scoring.py — RCA model validation"""
import pytest
from pydantic import ValidationError
from shared.models import RCAResult, ResolutionStep, Severity, IncidentFlow
from uuid import uuid4
from datetime import datetime

def make_rca(**overrides):
    defaults = dict(
        run_id=uuid4(), incident_id=uuid4(),
        external_id="P-TEST", severity=Severity.HIGH, flow=IncidentFlow.PRIMARY,
        root_cause="DB pool exhausted due to long-running batch job",
        confidence=0.87,
        resolution_steps=[ResolutionStep(step_num=1, action="Kill long-running queries")],
    )
    defaults.update(overrides)
    return RCAResult(**defaults)

def test_valid_rca():
    rca = make_rca()
    assert rca.confidence == 0.87
    assert len(rca.resolution_steps) == 1

def test_confidence_out_of_range_rejected():
    with pytest.raises(ValidationError):
        make_rca(confidence=1.5)

def test_confidence_lower_bound():
    with pytest.raises(ValidationError):
        make_rca(confidence=-0.1)

def test_rca_serialise_roundtrip():
    rca  = make_rca()
    json = rca.model_dump_json()
    rca2 = RCAResult.model_validate_json(json)
    assert rca2.external_id == rca.external_id
    assert rca2.confidence  == rca.confidence

@pytest.mark.parametrize("sev", ["P1","P2","P3","P4","P5"])
def test_all_severities_accepted(sev):
    rca = make_rca(severity=Severity(sev))
    assert rca.severity == Severity(sev)
