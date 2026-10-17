"""Skill-driven and drift/quarantine logging.

TODO: implement a simple logger (print or a text/log file under
outputs/artifacts/) that records, per batch: entity, batch id, rows read,
rows written to silver, rows quarantined (with reason), and any skill gap
identified/closed for that batch. This is your skill-driven evidence log.
"""


def log_event(entity: str, batch_id: str, message: str):
    """TODO: append a line to outputs/artifacts/pipeline_log.txt (or print),
    e.g. '[batch_3] billing_accounts: unrecognized signup_date format on
    ACC-0104, quarantined.'"""
    raise NotImplementedError
