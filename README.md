# 🚂 ROCC — Rail Operations Control Center
### Real-Time Rail Hazard Detection & Safety Communication Platform

> **Built for:** Union Pacific Railroad (UPRR) / LTTS Technical Interview
> **Stack:** Java 17 · Spring Boot 3.2 · Angular 19 · Apache Kafka · PostgreSQL/PostGIS · OAuth2

---

## Table of Contents
1. [Business Context](#1-business-context)
2. [Architecture Overview](#2-architecture-overview)
3. [Service Topology](#3-service-topology)
4. [Tech Stack](#4-tech-stack)
5. [Latency Budget](#5-latency-budget)
6. [Security Model](#6-security-model)
7. [Kafka Event Architecture](#7-kafka-event-architecture)
8. [Hazard Decision Matrix](#8-hazard-decision-matrix)
9. [AffectedTrainResolver Algorithm](#9-affectedtrainresolver-algorithm)
10. [Fail-Safe Operations](#10-fail-safe-operations)
11. [Quick Start — How to Import into IntelliJ](#11-quick-start--how-to-import-into-intellij)
12. [Architecture Discussion Guide (Interview Q&A)](#12-architecture-discussion-guide)
13. [Custom Metrics](#13-custom-metrics)
14. [Project Structure](#14-project-structure)

---

## 1. Business Context

Environmental conditions disrupt railroad operations daily. When a locomotive crew encounters a
wildfire, flash flood, rockslide, track washout, or bridge failure, the system must
**immediately notify all trains approaching the danger zone**.

Critical operational constraint:
> **Freight trains require 1.5–2 miles of stopping distance at speed.**
> Hazard decisions must propagate **end-to-end in under 2 seconds.**

### Regulatory Context
- **FRA (Federal Railroad Administration)** — complete audit trails required for all safety-critical decisions
- **GCOR (General Code of Operating Rules)** — train crews must acknowledge operational directives; acknowledgement records provide proof of compliance

---

## 2. Architecture Overview

```
┌──────────────────────────────────────────────────────────────────┐
│                    UPRR Operations Network                        │
│                                                                   │
│  Train Crew App ──┐                                              │
│  Dispatcher UI  ──┼──► API Gateway :8080 ──► Microservices      │
│  Monitoring UI  ──┘         │                                     │
│                        Spring Auth Server :9000                   │
│                        OAuth2 / OIDC (embedded)                   │
└──────────────────────────────────────────────────────────────────┘
```

| Concern | Strategy | Rationale |
|---|---|---|
| Hazard Detection | Event-Driven (Kafka) | Decoupled, durable, replayable |
| Notifications | At-Least-Once Delivery | Safety: never drop a directive |
| Fault Recovery | Retry + DLQ + Local Buffer | Multi-layer resilience |
| Consistency | Eventual Consistency | Latency over strong consistency |
| Safety Default | Fail-Safe (PROCEED_WITH_CAUTION) | FRA-aligned fail direction |
| Monitoring | OpenTelemetry + Micrometer | Vendor-neutral observability |
| Scalability | Horizontal (K8s HPA) | Partition by subdivisionId |
| Spatial Analysis | PostGIS (ST_DWithin) | O(log n) vs O(n) app-layer math |
| Authentication | OAuth2/OIDC (Spring Auth Server) | Enterprise IdP, on-premise |

---

## 3. Service Topology

| Service | Port | Responsibility |
|---|---|---|
| **API Gateway** | 8080 | Entry point, rate limiting, JWT validation, routing |
| **Authorization Server** | 9000 | OAuth2/OIDC token issuer (Spring Authorization Server) |
| **Hazard Reporting Service** | 8081 | Receive/validate hazards, lifecycle, Kafka producer |
| **Decision Engine Service** | 8082 | Rule-based severity classification, directive generation |
| **Train Communication Service** | 8083 | WebSocket/STOMP delivery, ACK tracking, retry + DLQ |
| **Train Tracking Service** | 8084 | Real-time positions, PostGIS spatial, AffectedTrainResolver |
| **Audit & Compliance Service** | 8085 | Append-only audit log, FRA/GCOR compliance |
| **Angular ROCC Dashboard** | 4200 | Dispatcher UI, live map, decision center |

| Infrastructure | Port | Purpose |
|---|---|---|
| PostgreSQL 15 + PostGIS | 5432 | Primary datastore |
| Apache Kafka | 9092 | Event backbone |
| Schema Registry | 8090 | Avro schema contracts |
| Jaeger | 16686 | Distributed tracing |
| Prometheus | 9090 | Metrics scraping |
| Grafana | 3000 | Dashboards |

---

## 4. Tech Stack

### Backend
| Technology | Version | Purpose |
|---|---|---|
| Java | 17 (LTS) | Platform |
| Spring Boot | 3.2.x | Application framework |
| Spring Cloud Gateway | 4.x | API Gateway |
| Spring Authorization Server | 1.2.x | OAuth2/OIDC provider |
| Spring Security | 6.x | Resource server JWT validation |
| Spring Data JPA | 3.x | ORM |
| Spring WebSocket | 6.x | STOMP |
| Spring Kafka | 3.x | Kafka + @RetryableTopic |
| PostgreSQL | 15 | Relational database |
| PostGIS | 3.4 | Spatial extension |
| Resilience4j | 2.x | Circuit breaker, retry, bulkhead |
| OpenTelemetry | 1.x | Distributed tracing |
| Micrometer | 1.12.x | Metrics → Prometheus |
| MapStruct | 1.5.x | DTO mapping |
| Lombok | 1.18.x | Boilerplate reduction |
| Flyway | 10.x | DB migrations |
| Testcontainers | 1.19.x | Integration tests |

### Frontend
| Technology | Version | Purpose |
|---|---|---|
| Angular | 19 | SPA framework |
| NgRx | 18.x | State management |
| @stomp/stompjs | 7.x | WebSocket/STOMP client |
| Leaflet | 1.9.x | Interactive map |
| angular-oauth2-oidc | 17.x | OAuth2 PKCE flow |

---

## 5. Latency Budget

Target: **< 2,000ms end-to-end** (hazard report → train notification)

```
t=0ms    Hazard report received at API Gateway
t=50ms   Kafka publish: hazard-reported
t=150ms  Decision Engine: consume + DB rule query (cache hit)
t=200ms  Kafka publish: decision-generated
t=250ms  Train Communication: consume event
t=300ms  WebSocket push to train onboard system
t=400ms  Angular dispatcher dashboard updated
─────────────────────────────────────────────
t=400ms  ACTUAL (nominal path, p95)
t=2000ms SLA LIMIT
1,600ms  Safety margin remaining (4× nominal)
```

---

## 6. Security Model

### OAuth2 Flows
| Flow | Used By | Purpose |
|---|---|---|
| Authorization Code + PKCE | Angular dashboard | Dispatcher login |
| Client Credentials | Service-to-service | Internal API calls |
| Refresh Token | Angular | Silent session renewal |

### Roles
| Role | Permissions |
|---|---|
| `ROLE_DISPATCHER` | View hazards, submit overrides, manage lifecycle |
| `ROLE_OPERATOR` | Read-only operations visibility |
| `ROLE_ADMIN` | All + rule engine admin + user management |
| `ROLE_TRAIN_CLIENT` | Submit hazard reports, receive notifications, ACK |

### Route Authorization
| Endpoint | Required Role |
|---|---|
| `POST /api/hazards` | DISPATCHER, TRAIN_CLIENT |
| `POST /api/overrides` | DISPATCHER only |
| `GET /api/audit/**` | DISPATCHER, ADMIN |
| `GET /api/admin/**` | ADMIN only |

---

## 7. Kafka Event Architecture

### End-to-End Flow
```
[Train Crew] → POST /hazards → [Hazard Service]
                                      │
                              ┌───────▼────────┐
                              │ hazard-reported │ (Avro)
                              └───────┬────────┘
                    ┌─────────────────┼──────────────┐
                    ▼                 ▼              ▼
           [Decision Engine]   [Audit Service] [Train Tracking]
                    │
            ┌───────▼────────┐
            │decision-generated│ (Avro)
            └───────┬────────┘
                    │
           [Train Communication]
                    │
            [WebSocket/STOMP] → [Train Onboard]
                    │
         ┌──────────▼──────────┐
         │notification-acknowledged│
         └──────────┬──────────┘
                    ▼
            [Audit Service] → append-only log
```

### Topics
| Topic | Producer | Consumers |
|---|---|---|
| `hazard-reported` | Hazard Reporting | Decision Engine, Audit, Train Tracking |
| `hazard-classified` | Decision Engine | Audit |
| `decision-generated` | Decision Engine | Train Communication, Audit |
| `notification-created` | Train Communication | Audit |
| `notification-delivered` | Train Communication | Audit |
| `notification-acknowledged` | Train Communication | Audit |
| `dispatcher-override` | Decision Engine | Audit |
| `train-location-updated` | Train Tracking | Train Communication, Audit |
| `dead-letter-events` | All (DLQ) | Alert Consumer |

### Retry Strategy
```
hazard-reported
  ├── hazard-reported-retry-1  (200ms)
  ├── hazard-reported-retry-2  (400ms)
  └── hazard-reported-dlt      → backup alert triggered
```

---

## 8. Hazard Decision Matrix

Rules stored in `decision_rule` table — **zero hardcoded switch/case logic**.

| Priority | Severity | Hazard Type | Action | Speed MPH |
|---|---|---|---|---|
| 1 | CRITICAL | ANY | EMERGENCY_STOP | — |
| 2 | HIGH | ANY except SIGNAL_FAILURE | STOP | — |
| 3 | HIGH | SIGNAL_FAILURE | SPEED_RESTRICTION | 10 |
| 4 | MEDIUM | FLASH_FLOOD | SPEED_RESTRICTION | 25 |
| 5 | MEDIUM | SNOW_ACCUMULATION | SPEED_RESTRICTION | 30 |
| 6 | MEDIUM | ANY | SPEED_RESTRICTION | 20 |
| 7 | LOW | ANY | PROCEED_WITH_CAUTION | — |

---

## 9. AffectedTrainResolver Algorithm

```
Input:  HazardReport { location, subdivisionId, impactRadiusMiles }
Output: List<Train>  { trains that must receive the directive }

Step 1 — Subdivision Filter
  SELECT * FROM train WHERE subdivision_id = :subdivisionId AND status = 'ACTIVE'

Step 2 — Spatial Proximity (PostGIS ST_DWithin)
  WHERE ST_DWithin(location::geography, ST_MakePoint(:lon,:lat)::geography, :radiusMeters)

Step 3 — Direction Filter (SAFETY-CRITICAL)
  bearing   = DEGREES(ST_Azimuth(train.location, hazard.location))
  angleDiff = ABS(((bearing - train.direction_degrees + 180) % 360) - 180)
  INCLUDE if angleDiff <= 90  (heading toward hazard)
  EXCLUDE if angleDiff >  90  (heading away)
  ⚠ SAFETY OVERRIDE: stale data > 60s → INCLUDE (unknown = unsafe)

Step 4 — Mile Post Filter (belt-and-suspenders)
  NORTHBOUND: EXCLUDE if train.mile_post > hazard.mile_post (already passed)
  SOUTHBOUND: EXCLUDE if train.mile_post < hazard.mile_post (already passed)
```

---

## 10. Fail-Safe Operations

| Failure | Immediate Action | Recovery |
|---|---|---|
| Decision Engine down | Auto-issue PROCEED_WITH_CAUTION | Events buffered → replay on recovery |
| Train Tracking down | Broadcast to full subdivision | Direction filtering restored on recovery |
| Kafka down | Write to `local_event_store` DB table | FailsafeEventScheduler replays every 30s |
| Notification fails | Retry ×3 with backoff | DLQ → backup email/SMS alert |
| WebSocket lost | Buffer server-side per trainId | Flush buffer on reconnect |

---

## 11. Quick Start — How to Import into IntelliJ


### Step 1 — Prerequisites
```bash
# Required
Java 17+      (verify: java -version)
Maven 3.9+    (verify: mvn -version)
Docker 24+    (verify: docker -version)
Node 20+      (verify: node -version)   # for Angular only
```

### Step 2 — Open in IntelliJ
```
1. File → Open
2. Navigate to: rail-hazard-platform/
3. Select the ROOT pom.xml
4. Click "Open as Project"
5. When prompted: "Open as Maven Project" → YES
6. Wait for indexing (first time: ~3-5 minutes, downloads dependencies)
```

### Step 3 — Start Infrastructure (Docker)
```bash
cd rail-hazard-platform
docker-compose up -d postgresql kafka zookeeper schema-registry jaeger prometheus grafana
```

### Step 4 — Run Services (in order)
Either use IntelliJ Run Configurations (pre-configured in `.run/` folder) or:
```bash
# Terminal 1
cd hazard-reporting-service && mvn spring-boot:run

# Terminal 2
cd decision-engine-service && mvn spring-boot:run

# Terminal 3
cd train-tracking-service && mvn spring-boot:run

# Terminal 4
cd train-communication-service && mvn spring-boot:run

# Terminal 5
cd audit-compliance-service && mvn spring-boot:run

# Terminal 6
cd api-gateway && mvn spring-boot:run
```

### Step 5 — Test the Full Flow
```bash
# Get OAuth2 token
TOKEN=$(curl -s -X POST http://localhost:9000/oauth2/token \
  -H "Content-Type: application/x-www-form-urlencoded" \
  -d "grant_type=client_credentials&client_id=rocc-client&client_secret=rocc-secret&scope=rocc.write" \
  | jq -r '.access_token')

# Submit CRITICAL wildfire hazard
curl -X POST http://localhost:8080/api/hazards \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "reportingTrainId": "550e8400-e29b-41d4-a716-446655440000",
    "subdivisionId": "laupt-sub-001",
    "hazardType": "WILDFIRE",
    "severity": "CRITICAL",
    "latitude": 34.0522,
    "longitude": -118.2437,
    "description": "Active wildfire across track at mile post 142",
    "impactRadiusMiles": 5.0,
    "idempotencyKey": "wildfire-20240115-001"
  }'

# Expected: 201 Created + hazardId
# Expected Kafka: hazard-reported event within 50ms
# Expected directive: EMERGENCY_STOP within 400ms
```

### Access Points
| UI | URL | Default Credentials |
|---|---|---|
| Angular Dashboard | http://localhost:4200 | dispatcher / dispatcher123 |
| Jaeger Tracing | http://localhost:16686 | — |
| Grafana | http://localhost:3000 | admin / admin |
| Prometheus | http://localhost:9090 | — |

---

## 12. Architecture Discussion Guide

### Q: "Why event-driven for a safety-critical system?"
Event-driven is *safer* for railroad ops: Kafka's durable log ensures no hazard is lost if
Decision Engine is temporarily down. Synchronous REST to a down service silently fails.
Decoupling enables independent fail-safes. Replayability allows bug fixes without data loss.
The 2s SLA is met with 1.6s safety margin on the nominal path.

### Q: "What happens if the Decision Engine goes down?"
Three layers: (1) Circuit breaker opens → FallbackFactory auto-issues PROCEED_WITH_CAUTION
within 2s SLA. (2) Hazard event still in Kafka → Decision Engine replays on recovery.
(3) ROLE_ADMIN alerted, dispatcher dashboard shows degraded-mode warning banner.
The train still gets a directive even with total Decision Engine failure.

### Q: "Walk me through AffectedTrainResolver."
4 filters: subdivision → PostGIS spatial proximity → direction-bearing filter (±90° of
hazard bearing = heading toward) → mile post filter. Safety override: stale location data
(>60s) → always include. PostGIS GiST index means O(log n) at 1,700+ locomotives.

### Q: "How do you ensure a notification is never lost?"
Four layers: (1) Kafka @RetryableTopic ×3 → DLQ → backup alert. (2) WebSocket offline
buffer per trainId → flush on reconnect. (3) Dispatcher dashboard UNDELIVERED badge.
(4) Audit record proves every notification attempt with timestamp.

### Q: "How would this scale to 1,700+ locomotives?"
Kafka partitioned by subdivisionId (~40 UPRR subdivisions). K8s HPA scales Train
Communication pods on WebSocket connections. PostGIS ST_DWithin with GiST index: <5ms
at fleet scale. Decision Engine is stateless — rule cache in Caffeine, multiple replicas.

### Q: "What would you build next?"
PTC/ATCS protocol adapters for automatic speed enforcement. ML severity prediction from
historical data. NOAA/NASA FIRMS integration for predictive hazard scoring. Mobile crew
app. Full track topology graph (PostGIS network or Neo4j) replacing subdivisionId model.

### Q: "How does this align with FRA/GCOR?"
Audit Service: append-only table, no UPDATE/DELETE on service account. Full chain of
custody per hazardId + traceId. Dispatcher overrides permanently recorded with reason.
Notification acknowledgement timestamps provide GCOR directive-receipt proof.

---

## 13. Custom Metrics

| Metric | Type | Tags |
|---|---|---|
| `hazards_received_total` | Counter | hazard_type, severity |
| `hazards_processed_total` | Counter | action |
| `decision_generation_latency_ms` | Histogram | — |
| `notifications_sent_total` | Counter | delivery_status |
| `notifications_acknowledged_total` | Counter | — |
| `active_hazards` | Gauge | — |
| `active_trains` | Gauge | — |
| `dispatcher_overrides_total` | Counter | original_action, new_action |
| `kafka_replay_events_total` | Counter | — |
| `circuit_breaker_state` | Gauge | service (0=CLOSED,1=OPEN,2=HALF_OPEN) |

---

## 14. Project Structure

```
rail-hazard-platform/
├── pom.xml                          ← ROOT Maven POM (import this into IntelliJ)
├── README.md
├── Architecture.md                  ← C4 diagrams, sequence diagrams (Mermaid)
├── DECISIONS.md                     ← 15 ADRs in MADR format
├── docker-compose.yml
│
├── api-gateway/                     ← Spring Cloud Gateway :8080
├── hazard-reporting-service/        ← :8081
├── decision-engine-service/         ← :8082
├── train-communication-service/     ← :8083
├── train-tracking-service/          ← :8084
├── audit-compliance-service/        ← :8085
│
├── rail-operations-control-center/  ← Angular 19 :4200
├── schemas/avro/                    ← Avro .avsc event schemas
├── infrastructure/k8s/              ← Kubernetes manifests + Helm
├── docs/openapi/                    ← OpenAPI 3.0 YAML per service
└── .github/workflows/               ← CI/CD pipelines
```
