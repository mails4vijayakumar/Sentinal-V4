-- ─────────────────────────────────────────────────────────────────────────────
--  routing-db/seed/dev_seed.sql  —  Development seed data
-- ─────────────────────────────────────────────────────────────────────────────

-- ServiceNow field mapping config (used by Agent 3)
INSERT INTO routing.snow_config (config_key, config_value, description) VALUES
  ('assignment_group_p1', 'Critical-Response-Team',      'P1 SNOW assignment group'),
  ('assignment_group_p2', 'Platform-Engineering',        'P2 SNOW assignment group'),
  ('assignment_group_p3', 'Application-Support',         'P3 SNOW assignment group'),
  ('caller_id',           'sentinel.agent@hospital.org', 'Service account caller ID'),
  ('category',            'Software',                    'Default category'),
  ('impact_p1',           '1',                           'SNOW impact for P1'),
  ('impact_p2',           '2',                           'SNOW impact for P2'),
  ('impact_p3',           '3',                           'SNOW impact for P3'),
  ('state_in_progress',   '2',                           'In Progress state value'),
  ('state_resolved',      '6',                           'Resolved state value')
ON CONFLICT (config_key) DO NOTHING;

-- KB seed documents for RAG bootstrapping
INSERT INTO kb.documents (id, source_type, source_id, title, source_url, content, content_hash) VALUES
  (gen_random_uuid(), 'confluence', 'ehr-high-mem-001',
   'EHR High Memory — Runbook',
   'https://wiki.example.com/ehr-high-memory',
   'Symptoms: EHR heap > 85%. Diagnosis: Check /admin/sessions for session leak. Resolution: POST /admin/gc to trigger GC. If unresolved in 5 min: rolling pod restart. Escalate to DBA if connection pool exhausted.',
   'seed_kb_ehr_001'),
  (gen_random_uuid(), 'confluence', 'db-connpool-002',
   'DB Connection Pool Exhaustion',
   'https://wiki.example.com/db-connpool',
   'Symptoms: JDBC timeout, HikariPool exhausted. Steps: 1. Check pgbouncer stats: SHOW POOLS. 2. Kill blocking queries > 5 min. 3. Temporarily raise pool_size. 4. Review long-running transactions in pg_stat_activity.',
   'seed_kb_db_002'),
  (gen_random_uuid(), 'confluence', 'hl7-restart-003',
   'HL7 Interface Engine Restart Procedure',
   'https://wiki.example.com/hl7-restart',
   'Prerequisites: Notify clinical informatics. Steps: 1. Check queue depth: mirth --status. 2. Drain or pause channels. 3. systemctl restart mirth. 4. Verify channel reconnection < 60s. Escalation: page HL7 on-call.',
   'seed_kb_hl7_003'),
  (gen_random_uuid(), 'confluence', 'pacs-recovery-004',
   'PACS Image Service Recovery',
   'https://wiki.example.com/pacs-recovery',
   'On PACS unavailable: 1. Check Orthanc: systemctl status orthanc. 2. Verify NFS: df -h /var/pacs. 3. Test DICOM port 4242. 4. tail -500 /var/log/orthanc/orthanc.log. 5. Restart if clean.',
   'seed_kb_pacs_004'),
  (gen_random_uuid(), 'confluence', 'k8s-oom-005',
   'Kubernetes OOMKilled Investigation',
   'https://wiki.example.com/k8s-oom',
   'OOMKilled resolution: 1. kubectl describe pod <name>. 2. Review resource limits and requests. 3. Check HPA: kubectl get hpa. 4. Immediate: increase memory limit 25%. 5. Long-term: profile with async-profiler. File Jira for capacity review.',
   'seed_kb_oom_005')
ON CONFLICT (content_hash) DO NOTHING;

-- Demo completed incidents for dashboard smoke test
DO $$
DECLARE
  v_inc1 UUID := gen_random_uuid();
  v_inc2 UUID := gen_random_uuid();
  v_run1 UUID := gen_random_uuid();
  v_run2 UUID := gen_random_uuid();
BEGIN
  -- Primary flow (P2 DT)
  INSERT INTO routing.incidents (id, external_id, source, severity, flow, title, service)
  VALUES (v_inc1, 'P-SEED-001', 'dynatrace', 'P2', 'primary',
          'EHR API — Response time degradation detected', 'ehr-api-service')
  ON CONFLICT (external_id) DO NOTHING;

  INSERT INTO routing.pipeline_runs (id, incident_id, status, flow, started_at, completed_at, duration_ms)
  VALUES (v_run1, v_inc1, 'completed', 'primary',
          NOW() - INTERVAL '2 hours', NOW() - INTERVAL '2 hours' + INTERVAL '47 seconds', 47000)
  ON CONFLICT DO NOTHING;

  -- Secondary flow (P4 SNOW)
  INSERT INTO routing.incidents (id, external_id, source, severity, flow, title, service)
  VALUES (v_inc2, 'INC0012345', 'servicenow', 'P4', 'secondary',
          'Storage controller — non-critical volume nearing capacity', 'san-ctrl-02')
  ON CONFLICT (external_id) DO NOTHING;

  INSERT INTO routing.pipeline_runs (id, incident_id, status, flow, started_at, completed_at, duration_ms)
  VALUES (v_run2, v_inc2, 'completed', 'secondary',
          NOW() - INTERVAL '3 hours', NOW() - INTERVAL '3 hours' + INTERVAL '18 seconds', 18000)
  ON CONFLICT DO NOTHING;
END;
$$;

SELECT 'Seed complete' AS result,
       (SELECT count(*) FROM routing.snow_config) AS snow_config_rows,
       (SELECT count(*) FROM kb.documents)         AS kb_documents;
