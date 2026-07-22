# Compatibility fixtures

This directory holds request/response fixtures used to compare the MVP against
the Confluent REST Proxy producer API shape.

The fixtures include project-owned golden cases plus selected behavior captured
from the local Confluent REST Proxy comparison service. Nondeterministic fields
such as offsets are normalized through fake producer results.

The next step is to recapture broader request-shape edge cases from the exact
Confluent REST Proxy version used in production, then freeze those normalized
expectations here.
