import pytest
from agents.Agent_8_knowledge_synth.phi_scrub import scrub_phi


@pytest.mark.unit
@pytest.mark.parametrize("inp,expected_marker", [
    ("Patient MRN-12345678 reported issue",          "[MRN REDACTED]"),
    ("Member id: 998877665 affected",                "[PATIENT-ID REDACTED]"),
    ("DOB 03/12/1980 in chart",                       "[DOB REDACTED]"),
    ("SSN 123-45-6789 was in log",                    "[ID REDACTED]"),
    ("NPI 1234567890 from provider",                  "[NPI REDACTED]"),
    ("Diagnosis ICD-10 E11.65 noted",                 "[DIAGNOSIS-CODE REDACTED]"),
])
def test_scrub_phi_redacts_known_patterns(inp, expected_marker):
    cleaned, count = scrub_phi(inp)
    assert expected_marker in cleaned
    assert count == 1


@pytest.mark.unit
def test_scrub_phi_leaves_safe_text_alone():
    cleaned, count = scrub_phi("HikariCP pool exhausted at 14:23 ET")
    assert cleaned == "HikariCP pool exhausted at 14:23 ET"
    assert count == 0


@pytest.mark.unit
def test_scrub_phi_counts_multiple_redactions():
    cleaned, count = scrub_phi("MRN-12345678 and SSN 111-22-3333")
    assert count == 2
