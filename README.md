<h1>Scalable Event-Driven Ingestion Pipeline for Real-Time Analytics</h1>



<h2>Overview</h2>

<p>

This project implements a <strong>scalable, event-driven data ingestion pipeline</strong>

designed to process simulated real-time sensor telemetry.

</p>



<p>

The system validates, streams, processes, and persists events using

<strong>Apache Kafka</strong> and <strong>MongoDB</strong>,

and exposes an API for analytical queries.

</p>



<div class="highlight">

The architecture follows modern data engineering best practices:

loose coupling, fault tolerance, idempotent processing,

and fully containerized deployment.

</div>



<h2>Architecture \& Data Flow</h2>



<pre>

+-----------+     +-------------------+     +---------+

| Generator | --> | Ingestion Service | --> | Kafka   |

+-----------+     +-------------------+     +----+----+

&nbsp;                                                    |

&nbsp;                                                    v

&nbsp;                                          +------------------+

&nbsp;                                          | Consumer Service |

&nbsp;                                          +--------+---------+

&nbsp;                                                   |

&nbsp;                                                   v

&nbsp;                                               +---------+

&nbsp;                                               | MongoDB |

&nbsp;                                               +----+----+

&nbsp;                                                    |

&nbsp;                                                    v

&nbsp;                                                +----------+

&nbsp;                                                |   API    |

&nbsp;                                                +----------+

</pre>



<h2>Services</h2>



<h3>1. Generator Service</h3>

<ul>

&nbsp;   <li>Simulates real-time sensor telemetry</li>

&nbsp;   <li>Configurable generation rate</li>

&nbsp;   <li>Sends JSON payloads to ingestion service</li>

&nbsp;   <li>Exposes <code>/health</code> endpoint</li>

</ul>



<h3>2. Ingestion Service (FastAPI)</h3>

<ul>

&nbsp;   <li>Validates incoming data using Pydantic schemas</li>

&nbsp;   <li>Rejects invalid records with HTTP 400</li>

&nbsp;   <li>Publishes validated events asynchronously to Kafka</li>

&nbsp;   <li>Structured JSON logging</li>

&nbsp;   <li>Exposes <code>/ingest</code> and <code>/health</code> endpoints</li>

</ul>



<h3>3. Kafka + Zookeeper</h3>

<ul>

&nbsp;   <li>Central event streaming backbone</li>

&nbsp;   <li>Decouples producers and consumers</li>

&nbsp;   <li>Ensures reliable message delivery</li>

</ul>



<h3>4. Consumer Service</h3>

<ul>

&nbsp;   <li>Subscribes to Kafka topic</li>

&nbsp;   <li>Processes events and persists them to MongoDB</li>

&nbsp;   <li>Implements strict idempotency using a compound unique index</li>

&nbsp;   <li>Safe against Kafka retries and consumer restarts</li>

&nbsp;   <li>Exposes <code>/health</code> endpoint</li>

</ul>



<h3>5. API Service</h3>

<ul>

&nbsp;   <li>Queries MongoDB for processed sensor data</li>

&nbsp;   <li>Supports filtering by <code>sensor\_id</code> and timestamp range</li>

&nbsp;   <li>Exposes <code>/data</code> and <code>/health</code> endpoints</li>

</ul>



<h2>Idempotency Strategy</h2>



<p>

Each sensor event is uniquely identified by:

</p>



<pre>

sensor\_id + timestamp

</pre>



<div class="success">

MongoDB enforces idempotency using a compound unique index.

Duplicate Kafka deliveries are safely ignored without data corruption.

</div>



<h2>MongoDB Schema \& Indexing</h2>



<ul>

&nbsp;   <li>Database: <code>sensor\_db</code></li>

&nbsp;   <li>Collection: <code>events</code></li>

</ul>



<p>Compound unique index:</p>



<pre>

(sensor\_id, timestamp) UNIQUE

</pre>



<p>

This ensures efficient querying and strict duplicate prevention.

</p>



<h2>Running the Application</h2>



<h3>Prerequisites</h3>

<ul>

&nbsp;   <li>Docker Desktop</li>

&nbsp;   <li>Git</li>

</ul>



<h3>Start All Services</h3>



<pre>

docker compose build

docker compose up -d

</pre>



<h2>API Usage Examples</h2>



<h3>Ingest Sensor Data</h3>



<pre>

curl -X POST http://localhost:8000/ingest \\

&nbsp; -H "Content-Type: application/json" \\

&nbsp; -d '{

&nbsp;   "sensor\_id": "sensor\_1",

&nbsp;   "timestamp": "2026-01-21T10:00:00Z",

&nbsp;   "temperature": 25.4,

&nbsp;   "humidity": 60.2,

&nbsp;   "location": {

&nbsp;     "latitude": 12.97,

&nbsp;     "longitude": 77.59

&nbsp;   }

&nbsp; }'

</pre>



<h3>Query Stored Data</h3>



<pre>

curl "http://localhost:8001/data?sensor\_id=sensor\_1"

</pre>



<h3>Verify Data Persistence</h3>



<pre>

docker exec -it gpp-event-pipeline-mongodb-1 mongosh -u root -p rootpassword

use sensor\_db

db.events.find().limit(5)

</pre>



<h2>Health Checks</h2>



<pre>

curl http://localhost:8000/health

curl http://localhost:8001/health

curl http://localhost:8002/health

curl http://localhost:8003/health

</pre>



<h2>Testing</h2>



<pre>

pytest

</pre>



<p>Unit tests validate:</p>

<ul>

&nbsp;   <li>Ingestion schema validation</li>

&nbsp;   <li>Consumer idempotent behavior</li>

</ul>



<h2>Technology Stack</h2>



<ul>

&nbsp;   <li>Python</li>

&nbsp;   <li>FastAPI</li>

&nbsp;   <li>Apache Kafka</li>

&nbsp;   <li>MongoDB</li>

&nbsp;   <li>Docker</li>

&nbsp;   <li>Pytest.</li>

</ul>



