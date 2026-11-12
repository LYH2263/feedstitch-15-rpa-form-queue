"""RPA form-entry queue (implemented).

Simulates a local robot typing fixture rows into an entry form one step at a time —
no real browser and no live network.

Flow:
- ``app.services.rpa_service.create_queue`` parses a local fixture leniently and turns
  each row into an ``rpa_steps`` row (status pending/success/failed) under an
  ``rpa_batches`` queue with an execution pointer.
- ``execute_queue`` fills pending steps in order (one step or a batch); successful steps
  either merge straight into canonical data or land in the ``intake_rows`` pre-ingest
  queue. Same-key / non-key divergences still go to conflict quarantine; failed steps
  are never written as ingested data.
- ``promote_intake`` merges held intake rows later using the same merge rules.
- Batch reports derive every count from ``rpa_steps`` and cross-check against
  staging_rows / conflicts / intake_rows / canonical_items so the numbers reconcile.
"""
