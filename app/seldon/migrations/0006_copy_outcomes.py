"""Copy stored forecasts and refusals into per-head outcomes.

Before the model contract, a forecast held every output of one checkpoint
for one detection, and a refusal declined all of them. The contract stores
one outcome per detection, deployment, and head. This migration gives each
checkpoint found in the old tables a retired ``seldon-pre-contract``
deployment with its three heads enabled from the start of time, then copies:

- each forecast into a flux-forecast, a classification, and a
  basis-parameters product, with its regime columns as caveats;
- each refusal into a refusal for each of the three heads.

The latent mean and sigma are not copied: embeddings are transient, so this
drop is deliberate and cannot be undone from the new tables.

Detections already owed (including those forecast or refused) get the time
they became owed, taken from their earliest stored outcome or else their
earliest delivery, and the qualification snapshot from their earliest
forecast. Detection states are left as they are.

Everything is set-based SQL, so the copy does not walk rows in Python.
"""

from django.db import migrations

PRE_CONTRACT = "seldon-pre-contract"

ROMAN_WFI = ("F062", "F087", "F106", "F129", "F146", "F158", "F184", "F213")


def survey_pairs(bands: str) -> str:
    """Build SQL turning a JSON list of band names into survey-band pairs.

    A one-letter band is LSST's, a Roman WFI filter is Roman's, and any
    other band is JWST's, as the checkpoint names them.

    Args:
        bands: SQL for a ``jsonb`` array of band names.

    Returns:
        SQL for a ``jsonb`` array of ``[survey, band]`` pairs, in order.
    """
    roman = ", ".join(f"'{band}'" for band in ROMAN_WFI)
    return (
        "COALESCE((SELECT jsonb_agg(jsonb_build_array("
        "CASE WHEN length(e.band) = 1 THEN 'lsst' "
        f"WHEN e.band IN ({roman}) THEN 'roman' ELSE 'jwst' END, e.band) "
        "ORDER BY e.n) "
        f"FROM jsonb_array_elements_text({bands}) WITH ORDINALITY AS e(band, n)), "
        "'[]'::jsonb)"
    )


PRE_CONTRACT_CLAIMS = (
    "SELECT c.id FROM seldon_outcomeclaim c "
    "JOIN seldon_modeldeployment d ON d.id = c.deployment_id "
    f"WHERE d.package_name = '{PRE_CONTRACT}'"
)

FORWARD = [
    # One retired deployment per checkpoint.
    f"""
    INSERT INTO seldon_modeldeployment (
        package_name, package_version, contract_version, artifact_digest,
        declaration, registered_at, last_heartbeat_at, retired_at
    )
    SELECT '{PRE_CONTRACT}', checkpoint, '0.0', 'pre-contract',
           '{{}}'::jsonb, now(), NULL, now()
    FROM (
        SELECT checkpoint FROM seldon_forecast
        UNION
        SELECT checkpoint FROM seldon_refusal
    ) AS checkpoints
    """,
    # Its three heads, enabled for every detection ever owed.
    f"""
    INSERT INTO seldon_headrevision (
        deployment_id, head, enabled, inputs, effective_at, created_at
    )
    SELECT d.id, h.head, true, '{{}}'::jsonb,
           '1970-01-01T00:00:00Z'::timestamptz, now()
    FROM seldon_modeldeployment d
    CROSS JOIN (
        VALUES ('flux_forecast'), ('classification'), ('basis_parameters')
    ) AS h(head)
    WHERE d.package_name = '{PRE_CONTRACT}'
    """,
    # A claim per head for each forecast, then for each refusal.
    *(
        f"""
        INSERT INTO seldon_outcomeclaim (
            detection_id, deployment_id, head, revision_id, state, attempts,
            leased_by, lease_expires_at, created_at, updated_at
        )
        SELECT o.detection_id, d.id, r.head, r.id, '{state}', 1,
               '', NULL, o.stored_at, o.stored_at
        FROM {table} o
        JOIN seldon_modeldeployment d
          ON d.package_name = '{PRE_CONTRACT}' AND d.package_version = o.checkpoint
        JOIN seldon_headrevision r ON r.deployment_id = d.id
        """
        for table, state in (
            ("seldon_forecast", "product"),
            ("seldon_refusal", "refusal"),
        )
    ),
    # Each forecast's three products, with its regime as caveats.
    f"""
    INSERT INTO seldon_product (
        claim_id, output_kind, payload, caveats, light_curve, app_version,
        stored_at
    )
    SELECT c.id,
           CASE c.head
               WHEN 'flux_forecast' THEN 'flux_series'
               WHEN 'classification' THEN 'class_probabilities'
               ELSE 'named_values'
           END,
           CASE c.head
               WHEN 'flux_forecast' THEN jsonb_build_object(
                   'times', f.grid -> 'eval_times',
                   'bands', {survey_pairs("f.grid -> 'eval_bands'")},
                   'flux', f.outputs -> 'flux',
                   'flux_err', f.outputs -> 'flux_err',
                   'zero_point', f.outputs -> 'zero_point_mag',
                   'eval_span_days', f.eval_span_days,
                   'eval_span_in_time_scales', f.eval_span_in_time_scales
               )
               WHEN 'classification' THEN jsonb_build_object(
                   'probabilities', f.outputs -> 'class_probabilities'
               )
               ELSE jsonb_build_object('values', f.outputs -> 'parameters')
           END,
           CASE WHEN f.sparse THEN jsonb_build_array(jsonb_build_object(
               'code', 'sparse_light_curve',
               'detail', jsonb_build_object(
                   'n_observations', f.n_observations,
                   'min_points', f.min_points
               )
           )) ELSE '[]'::jsonb END
           || CASE WHEN jsonb_array_length(f.untrained_bands) > 0
               THEN jsonb_build_array(jsonb_build_object(
                   'code', 'untrained_band',
                   'detail', jsonb_build_object(
                       'bands', {survey_pairs("f.untrained_bands")}
                   )
               )) ELSE '[]'::jsonb END,
           f.light_curve || jsonb_build_object('zero_point', f.zero_point),
           f.app_version,
           f.stored_at
    FROM seldon_outcomeclaim c
    JOIN seldon_modeldeployment d ON d.id = c.deployment_id
    JOIN seldon_forecast f
      ON f.detection_id = c.detection_id AND f.checkpoint = d.package_version
    WHERE d.package_name = '{PRE_CONTRACT}' AND c.state = 'product'
    """,
    # Each refusal, once per head.
    f"""
    INSERT INTO seldon_headrefusal (
        claim_id, code, message, detail, app_version, stored_at
    )
    SELECT c.id, o.code, o.message, jsonb_build_object('bands', o.bands),
           o.app_version, o.stored_at
    FROM seldon_outcomeclaim c
    JOIN seldon_modeldeployment d ON d.id = c.deployment_id
    JOIN seldon_refusal o
      ON o.detection_id = c.detection_id AND o.checkpoint = d.package_version
    WHERE d.package_name = '{PRE_CONTRACT}' AND c.state = 'refusal'
    """,
    # When each owed detection became owed.
    """
    UPDATE seldon_detection AS det
    SET owed_at = COALESCE(
        (
            SELECT min(o.stored_at) FROM (
                SELECT stored_at FROM seldon_forecast
                WHERE detection_id = det.id
                UNION ALL
                SELECT stored_at FROM seldon_refusal
                WHERE detection_id = det.id
            ) AS o
        ),
        (
            SELECT min(received_at) FROM seldon_delivery
            WHERE detection_id = det.id
        ),
        now()
    )
    WHERE det.state IN ('owed', 'forecast', 'refused') AND det.owed_at IS NULL
    """,
    # Why it was owed, from its earliest forecast.
    """
    UPDATE seldon_detection AS det
    SET qualification = jsonb_build_object(
        'thresholds', f.thresholds,
        'qualifying_event', f.qualifying_event
    )
    FROM (
        SELECT DISTINCT ON (detection_id)
               detection_id, thresholds, qualifying_event
        FROM seldon_forecast
        ORDER BY detection_id, stored_at, id
    ) AS f
    WHERE f.detection_id = det.id AND det.qualification = '{}'::jsonb
    """,
]

BACKWARD = [
    *(
        f"DELETE FROM {table} WHERE claim_id IN ({PRE_CONTRACT_CLAIMS})"
        for table in ("seldon_product", "seldon_headrefusal", "seldon_productfailure")
    ),
    f"DELETE FROM seldon_outcomeclaim WHERE id IN ({PRE_CONTRACT_CLAIMS})",
    "DELETE FROM seldon_headrevision WHERE deployment_id IN ("
    f"SELECT id FROM seldon_modeldeployment WHERE package_name = '{PRE_CONTRACT}')",
    f"DELETE FROM seldon_modeldeployment WHERE package_name = '{PRE_CONTRACT}'",
    "UPDATE seldon_detection SET owed_at = NULL, qualification = '{}'::jsonb",
]


class Migration(migrations.Migration):

    dependencies = [
        ("seldon", "0005_model_contract_schema"),
    ]

    operations = [
        migrations.RunSQL(FORWARD, reverse_sql=BACKWARD),
    ]
