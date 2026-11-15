# Concepts

> Shared domain vocabulary for this project — entities, named processes, and status concepts with project-specific meaning. Seeded with core domain vocabulary, then accretes as ce-compound captures and refreshes learnings; direct edits are fine. Glossary only, not a spec or catch-all.

## Alert intake

### Night
The 12:00-to-12:00 UTC period, following Rubin's dayObs, named by the date at its start; intake counts and reports are grouped by night.

### Qualifying broker
A broker whose scores (a reliability score and an SN-like score, each with its own cut) can make an object worth forecasting.

### Full-stream source
A broker read for every detection it publishes, so that detections are on hand before or without a qualifying broker naming them; it carries light curves but its deliveries never qualify an object.

Each delivery records which of the two roles its broker played. A broker plays one role.

### Qualified object
A Rubin object that one qualifying broker's scores on one of its detections passed. The cuts in effect at that moment are kept with the object, so later setting changes do not rewrite why it qualified.

### Detection
One Rubin measurement of an object (a Rubin `diaSource`) that the service has received from at least one broker.

Lifecycle: **pending** (received, its object not yet qualified on a detection observed at or before it), **owed** (a forecast is due), then **forecast** or **refused** (the model declined it, with a reason). A pending detection is held for a fixed period from its first delivery. When the hold ends, the detection is **missed** if its object qualified on a later detection, and discarded if its object never qualified. Each detection is forecast at most once per model checkpoint, however many brokers deliver it.

### Delivery
One broker's sending of one detection, with the light curve that alert carried and its publication and receipt times. A forecast is made from the earliest-received delivery's light curve. A broker that redelivers the same detection adds no second delivery.

### Intake skip
An alert that intake could not turn into a forecast request, such as one that could not be decoded or held a value the database refused. It is recorded with its broker and reason rather than dropped silently.

### Stream gap
A period when a broker's stream could not be read. It is **confirmed** when the transport shows messages were lost, and **suspected** when downtime or silence may have lost them.

### Intake lag
The time from a broker publishing an alert to the consumer committing the stored work for it, kept as counts per broker, night and lag bucket. The counts survive the deletion of the deliveries they describe.
