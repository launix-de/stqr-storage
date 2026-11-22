# STQR – Architektur- und Implementierungsspezifikation

**Status:** Architekturentwurf v0.9 · **Sprache der Implementierung:** Go · **Stand:** 20. November 2026

**Projektname:** **STQR** – Anspielung auf *store* und **SPQR**.

**Zweck:** Schlanker, hochskalierbarer, verteilbarer Object Store als universelle persistente Storage-Schicht für Datenbanken, Anwendungen, Dateisysteme und Virtualisierung.

**Normative Sprache:** **MUSS**, **SOLL**, **KANN** bezeichnen Verbindlichkeitsgrade. Mit **ENTSCHEIDUNG** markierte Punkte gelten für die Referenzarchitektur; **OFFEN** bezeichnet bewusst noch nicht abschließend entschiedene Details.

> **Entwurfsprinzip:** Ein einzelner STQR-Node ist ein vollständiger Object Store. Durch Hinzufügen von gleichberechtigten Nodes wächst er ohne Wechsel der Client-API zu einem verteilten Cluster. Clients berechnen Objekt-Hashes lokal, finden die zuständige Hash-Range in einer kleinen, versionierten Placement-Map und sprechen den Storage-Node direkt an. Die physische Datenplatzierung ist nicht durch den Hash festgeschrieben; Splits, Merges und Migrationen werden geplant und möglichst spät umgesetzt.

---

## 1. Ziele, Nichtziele und Randbedingungen

### 1.1 Produktziele

1. **Effiziente Speicherung unterschiedlicher Objektgrößen:** Beispiel-Workloads mit **2–250 KiB** großen Payloads (z. B. Spaltenwerte, Binärobjekte und Anwendungspayloads) begründen insbesondere das eigene Storage-Format statt einer OS-Datei je Objekt. Diese Größen sind **keine Objektgrößenbeschränkung**. STQR besitzt intern kein festes Objektgrößenlimit; große Objekte werden über Extents/Records und Streaming verarbeitet. CPU-, Speicher-, Index-, Netzwerk- und Write-Amplification sollen gering bleiben.
2. **Durability:** Für HDD-/SSD-Buckets ist eine erfolgreich bestätigte schreibende Operation nach dem definierten Durability-/Redundanzvertrag persistent; Verlust durch gewöhnlichen Prozessabsturz, OS-Neustart oder Stromausfall darf nicht durch fehlende Flushes entstehen. Geräte, die Flush-/FUA-Garantien verletzen, sind davon nicht abgedeckt. RAM-Buckets erhalten Daten solange mindestens eine gültige RAM-Replik lebt und stellen neu gestartete lokale Kopien aus diesen Replikaten wieder her; CACHE erlaubt später jederzeitigen Verlust (§8.1).
3. **Atomare Einzelobjektoperationen**, insbesondere vollständiges und ranged `PUT`, `DELETE`, `TRUNC` und ein **atomares `APPEND`**, für Transaktionslogs, Journale und andere append-lastige Anwendungen. STQR führt **keine Multi-Object-Datenbanktransaktionen** aus.
4. **Single-Node-first:** Betrieb auf einem Node und einer Volume-Datei ohne künstliche Mindestzahl an Nodes; später dynamisches Wachstum.
5. **Direktes Routing:** Client-SDKs können anhand lokal gespeicherter Placement-Maps direkt mit den zuständigen Nodes kommunizieren. Kein dauerhafter zentraler Datenproxy.
6. **Adaptive Hash-Ranges:** Binäres Splitten, Zusammenführen benachbarter Ranges, bytegenaue Kapazitäts- und lastbasierte Placement-Entscheidungen.
7. **Lazy Migration und Generationen:** Maximal `CURRENT` und `NEXT`; ohne Rebuild-Grund keine Migration. Rebalancing bezieht nur Online-Nodes ein; das Placement von Offline-Nodes bleibt unverändert. Während eines Rebuilds sammelt der Koordinator weitere Gründe in `needrebuild`. Ausführbare Folgegründe starten nach Abschluss den nächsten Lauf; von Offline-Nodes abhängige Gründe bleiben bis zu deren Wiederkehr vorgemerkt. Nur ein administrativer Forceful Disconnect darf die Mitgliedschaft unter Verlustannahme ändern.
8. **Gleichberechtigte Nodes:** Kein dauerhaft bevorzugter Master. Ein nur für die Generation gewählter Plan-Koordinator; der laufende Datenpfad ist unabhängig von ihm.
9. **Buckets und Multi-Tenancy:** Admin-gesteuerte Bucket-Policies, User-Freigaben, Quotas, HDD-/SSD-/RAM-Basisspeicher, optionale RAM-Read-Caches und später Erasure Coding.
10. **HTTP-Daemon mit Basic Auth**, integriertem Dashboard, administrativer HTTP-API und WebSocket-Upgrade. Jede binäre WebSocket-Nachricht enthält genau eine STQR-Nachricht; ein gemeinsamer Codec dient Clients, Admin und Nodes.
11. **Go-Implementierung** mit C-, Rust- und Go-Clientbibliotheken, FUSE-Frontend, lokal betreibbarem S3-Gateway, einer Blockdevice-Library zur späteren QEMU-Einbindung, optionalem NBD-Frontend sowie Admin-Dashboard.
12. **Umfassende automatisierte Verifikation:** Unit-, Property-, Recovery-, Fault-Injection-, Last- und Stresstests einschließlich sehr kleiner Test-Volumes im MiB-Bereich.

### 1.2 Nichtziele der ersten Iteration

- STQR ist **keine SQL-/KV-Datenbank** und bietet keine Abfrage- oder Multi-Object-Transaktionsengine.
- Keine verteilten Transaktionen über mehrere Objekte/Buckets.
- Eigene optimierte FUSE-/S3-/Blockdevice-Libs sind verbindlicher Umfang. Fremde Storage-ABI-/Wire-Drop-in-Kompatibilität ist kein Projektziel. S3 umfasst bereits im ersten Frontend-Release LIST und Multipart neben PUT/GET/HEAD/DELETE.
- Keine Garantie unbegrenzter Schreibverfügbarkeit bei Netzwerkpartitionen (Konsistenz und Durable Ownership haben Vorrang).
- Erasure Coding, komplexe Multi-Reader-Cache-Kohärenz und vollständige POSIX-Semantik sind zunächst **vorbereitet**, aber nicht Voraussetzung für ein erstes Release.
- „Zero Overhead“ ist ein **Optimierungsziel**, kein wörtlich erreichbarer Zustand: Authentifizierung, Framing, Checksummen, Index, Kopiergrenzen und Persistenz verursachen unvermeidliche Kosten.

### 1.3 Begriffe

| Begriff | Bedeutung |
|---|---|
| **Node** | Gleichberechtigter STQR-Serverprozess mit 128-Bit-Node-ID und optionalen Volumes. |
| **Volume** | Persistente Datei/Raw-Device mit STQR-Format oder als Volume gemessener flüchtiger RAM-Pool; RAMCACHE ist eine getrennte zusätzliche Read-Cache-Ressource. |
| **Bucket** | Isolierter Objektnamensraum samt Policy, Nutzungsstatistik und Zugriffsrechten. |
| **Object Key** | Innerhalb des Buckets eindeutiger, binärer Objektschlüssel. |
| **Object Hash** | Deterministisch aus Bucket-ID und Object Key berechnete 256-Bit-Adresse. |
| **Range** | Zusammenhängendes Hashintervall, beschrieben durch den **inklusiven Startwert**; das Ende ergibt sich aus dem nächsten Startwert. |
| **Range-Group** | Alle Locations/Replikate/Fragmente/Caches derselben Range. |
| **Placement-Map** | Versionierte, sortierte Zuordnung von (Bucket, Range) zu einer Storage-Policy und ihren Locations. |
| **Generation** | Geplanter, kontrollierter Wechsel von Placement und Bucket-Policies, einschließlich Migration. |
| **Ownership Epoch** | Monoton steigender Fencing-Stand für die Schreibautorität einer Range. |
| **CURRENT/NEXT** | Gegenwärtige autoritative Placement-Generation und vorbereitete Zielgeneration. |
| **Durable Replica** | Persistente Kopie, die zur zugesagten Redundanz zählt. |
| **Read Cache** | Entbehrliche, flüchtige oder sekundäre Kopie, die nicht zur Durability zählt. |

---

## 2. Architekturübersicht

```text
                       native apps / databases / VMs
                         |       |       |
                       Go SDK  C/Rust   S3/FUSE/NBD gateways
                         \       |       /
                          STQR native client
                                 |
               HTTP Basic + bucket policy + adaptive Range-Map
                                 |
              H(bucket-id || key) -> Range -> Replica/Primary
                                 |
                     direkter WebSocket-Datenpfad
                 +---------------+---------------+
                 |               |               |
               Node A          Node B          Node C
              HDD/SSD         HDD/SSD       SSD/RAMCACHE
                 |               |               |
            Segment+Index  Segment+Index   Segment+Index
                 \               |               /
                  | verteilte Steuerungsmetadaten |
                  +---------------+---------------+
                                  |
                        temporärer Koordinator
                 (Planung nur bei Generationenwechsel)
```

### 2.1 Komponenten eines Nodes

- **Transport/Protocol Server:** Go-HTTP-Server, Intranet-Basic-Auth, Dashboard/API, WebSocket-Nachrichten, Requests, Pushes und Streaming.
- **Object Engine:** atomare `GET`/`PUT`/`DELETE`/`APPEND`, Hash-Index, Disksegmente/RAM-Extents, Checkpoints, Group Commit und CAS.
- **Volume Manager:** Online-/Offline-Lifecycle, Volume-Header, Klasse, freier Speicher, Gesundheit, Flush, lokale Verteilung.
- **Range Owner:** persistente Epochs, explizites Fencing bei Übergaben, ALL-Replikation und Read-Kohärenz; keine zeitbasierte automatische Ablösung.
- **Migration Worker:** Snapshot/Copy, Änderungsnachlauf, sichere Umschaltung, Cleanup.
- **Statistics Collector:** pro Range, pro Volume, pro Node und Bucket Nutzdaten- und I/O-Messwerte.
- **Control-Plane Participant:** Membership, konsistente Metadaten, Wahl des Plan-Koordinators, Map-Publikation.
- **Admin-/Metrics-Endpunkte:** HTTP-API und WebSocket direkt im Daemon; eingebettetes Dashboard, identische Handler und Statistikstrukturen.

### 2.2 Verteilung von Zuständigkeiten

- Alle Nodes sind gleichberechtigt. Ein temporärer Generationskoordinator plant außerhalb des normalen Objekt-I/O-Pfads.
- **ENTWURFSFESTLEGUNG: kooperatives ALL-Protokoll**, kein Raft mit zeitgetriebener Leader-Neuwahl. Für einen Rebuild umfasst ALL die fest gebundene Online-Teilnehmermenge der Runde (§4.3); globale Membership-/Admin-Entscheidungen benötigen weiterhin alle erforderlichen Mitglieder (§4.5). Es handelt sich nicht um eine wechselnde Mehrheit. Der konkrete Race-Algorithmus der Koordinatorwahl einschließlich sicherer Teilnehmerbindung ist ausdrücklich bis nach Stresstests zurückgestellt (§4.2, §20.5). Statistik-Snapshots, Plan-Freigaben und Ownership-Entscheidungen sind wiederaufnehmbar.
- Ein Node genügt. Bereits vor einer Rebuild-Runde fehlende Mitglieder blockieren das Rebalancing unabhängiger Online-Ranges nicht; ihr Placement wird unverändert übernommen. Ein fehlender erforderlicher Teilnehmer einer bereits gebundenen Runde oder einer globalen Kontrollentscheidung blockiert dagegen deren Abschluss. Objekt-I/O kann auf unveränderten erreichbaren Replica-Sets weiterlaufen; abhängige Operationen warten.
- Nichterreichbarkeit entzieht niemals selbsttätig Mitgliedschaft, Ownership oder einen Koordinatorstatus. Es gibt keinen Failure-Timeout mit automatischem Ausschluss. Verbindungsfehler, Reconnect-Backoff und anwendungsseitige Cancellation sind zulässig und ändern diesen Vertrag nicht.
- Join, geplanter Leave und Forceful Disconnect wechseln die Membership-Epoch nach §4.5. Ein ausgeschlossener Node darf mit alter Epoch nicht wieder als gültiges Mitglied auftreten.

---

## 3. Node-Identität, Cluster-Join und Leave

### 3.1 Konfiguration und automatischer Start

```yaml
node:
  id: "92d6b5ef-c0ad-45ca-91f4-d731dc3e55ae"
  listen: ":7419"
  advertise: "http://192.0.2.10:7419"
  data_dir: "/var/lib/stqr"
auth:
  cluster_token: "<automatisch erzeugtes Cluster-Secret im Klartext>"
  bootstrap_admin:
    username: "admin"
    password: "<automatisch erzeugtes Passwort im Klartext>"
ram_bytes: 1073741824
peers:
  - node_id: "3ac67634-92fa-4338-a063-2beb89095e1b"
    url: "http://192.0.2.11:7419"
volumes:
  - path: "/srv/stqr/data.vol"
    class: ssd
    size_bytes: 1073741824
ramcache_bytes: 0
```

- Die lokale Einstellungsdatei beschreibt vor allem die eigene Node, ihre Volumes und einige bekannte Peers. Fehlt sie, legt der Daemon eine leere Datei an, ergänzt persistente Node-ID/Defaults und speichert nachfolgende lokale Änderungen atomar. Cluster-Token und initiales Admin-Passwort werden beim neuen Cluster automatisch erzeugt und **im Klartext in dieser lokalen Konfiguration** gespeichert; neue Accounts und weitere Admins werden als Passwortverifier im System-Bucket geführt (§5.7). Beim Join wird das vorhandene Cluster-Token in der Config verwendet; es wird kein abweichendes neues Cluster-Secret oder zweiter Bootstrap-Admin angelegt. Keine Credentials in veröffentlichten Maps.
- `stqr` startet den Daemon. Ohne bestehende Clusteridentität, bekannte Peers oder `--connect http://host:port` entsteht automatisch ein neuer Single-Node-Cluster; ein gesondertes `init` ist nicht nötig.
- Sind Peers konfiguriert, ist `--connect` gesetzt oder liegt bereits eine persistente Clusteridentität vor, verbindet sich die Node mit diesem Cluster bzw. wartet auf dessen Wiederkehr. Unerreichbare Peers dürfen **keinen versehentlichen neuen Cluster** auslösen. **`--connect` ist nur ein Helfer für die erste Kontaktaufnahme, kein Primary und keine dauerhaft bevorzugte Anlaufstelle.** Der entdeckte Peer-Stand liegt als normales Objekt im System-Bucket und ist beim Wiederanlauf maßgeblich; die lokale Config enthält höchstens einen abgeleiteten Cache bzw. anfängliche Kontaktinformationen. Nach Peer-Connect werden Identität/Adressen an die übrigen Nodes weiterverteilt. Nach komplettem Cluster-Stopp müssen beliebige verfügbare bekannte Nodes den Wiederverbund ermöglichen, auch wenn der ursprüngliche Initiator noch ausgeschaltet ist. Peer-Erkennung allein ändert keine committed Membership und reaktiviert insbesondere keine ausgeschlossene Node.
- Dashboard/API können Volumes direkt hinzufügen; validieren/formatieren/mounten und Speichern der Konfiguration geschehen im Daemon (§8.2). Ein separates Admin-CLI oder Map-Agent gehört nicht zu v1.

### 3.2 Bootstrap, Join und Wiederkehr

1. Ein neuer Single-Node-Cluster erhält Cluster-ID, Membership-Epoch 1 und eine persistente Metadatenbasis. Neue Buckets starten mit einer Range ab 0 auf der eigenen Node.
2. `--connect` bzw. konfigurierte Peers lösen einen authentifizierten `NODE_HELLO`/`JOIN` aus. Node-ID und Cluster-ID sind stabile Identitäten, URLs nur Kontaktadressen.
3. Der Seed pusht bekannte Membership/Generation/Policy-Stände. Der Join wird von allen bisherigen Mitgliedern bestätigt; der neue Node ist erst im committed Folgestand Mitglied und frühestens im nächsten Placement-Plan reguläres Storage-Ziel. System-/EVERYWHERE-Buckets werden gemäß §5.6/§5.7 vor ihrer Mitgliedsaktivierung vorab synchronisiert.
4. Nach Neustart wird der lokale Kontrollstand gelesen und mit `RESUME_STATE` der Peers abgeglichen. Diese teilen insbesondere mit, ob die Node Koordinator war und welche Fragmente/Plan-/Cutover-Entscheidungen vorliegen. Eine fehlende In-Memory-Erinnerung ist kein Grund zur Neuwahl.
5. Verlorener persistenter Kontrollzustand wird von übereinstimmenden Peer-Belegen wiederhergestellt, bevor die Node Autorität ausübt. Bei widersprüchlichen Belegen wird gewartet und ein administrativer Fehler gemeldet.

### 3.3 Geplanter Austritt und interner Draining-Zustand

- Ein **Graceful Leave** kann jederzeit angefordert werden, auch während einer laufenden Generation.
- Die Node setzt bei einem Abmeldewunsch ihren **internen Node-Zustand `DRAINING`**. Dies ist eine Planungsinformation; bestehendes Placement, Ownership und die Bedienung gültiger Requests bleiben zunächst wirksam. Der Zustand allein löst weder Redirects noch einen sofortigen Ownership-Wechsel aus.
- Der nächste **Generationen-Builder** erhält diesen Zustand gemeinsam mit den Load- und Storage-Statistiken bei deren Einsammlung (§7.1). Er plant die Entleerung der Node und weist ihr im Zielplan keine neuen Ranges, Cache-Kopien oder Primary-Rollen zu.
- Trifft der Abmeldewunsch während einer bereits geplanten oder laufenden Generation ein, wird er für den nächsten regulären Planungslauf vorgemerkt; der bestehende Plan wird dadurch nicht nachträglich geändert. Nichterreichbarkeit bedeutet Warten (§3.5); eine Membership-Änderung erfordert administrative Freigabe.
- Sobald der abmeldende Node den committed Übergangs-/Replikationsplan erhalten hat, arbeitet er bestehende Operationen kontrolliert ab. Für bereits abgegebene oder lokal nicht mehr gehaltene Ranges antwortet er mit 301/302 und einem gültigen Routing-Hinweis (§12.2). Noch vorhandene Daten darf er unter den dortigen Konsistenzregeln weiterhin ausliefern.
- Sobald alle benötigten Daten sicher abgegeben und alle laufenden Operationen beendet sind, sendet er auf allen Client-Verbindungen `GOAWAY` mit Grund `DRAINED`, aktueller Map-Version und Kontaktalternativen und schließt die Verbindungen kontrolliert.
- `LEAVE_COMPLETE` wird erst bestätigt, nachdem alle autoritativen und vertraglich erforderlichen Daten/Kopien sicher ersetzt beziehungsweise migriert wurden und die Membership konsistent angepasst ist.
- Ein **Forceful Disconnect** ist eine ausdrückliche administrative Verlustentscheidung nach §3.5; ein Ausfall allein löst ihn nicht aus.
- Ein Abmeldewunsch kann in jeder Generation erfasst werden; seine Umsetzung erfolgt anhand des nächsten darauf aufbauenden Generationsplans. Wenn kein sicherer Zielort verfügbar ist, bleibt der Austritt ausstehend oder erfordert ausdrücklich autorisierte Datenverlust-/Degradations-Policy.

### 3.4 Failure Domains

Node, Host, Rack und Zone sind optionale, administrativ gesetzte Failure-Domain-Tags. Repliken bzw. EC-Fragmente müssen entsprechend der Bucket-Policy auf verschiedene Failure Domains verteilt werden. Ein Node mit mehreren HDDs liefert dadurch nicht automatisch Host-Redundanz.

---

### 3.5 Shutdown-Modi und Forceful Disconnect

**Standard bei Prozess-/Systemende ist immer `SHORT` („Warte auf mich“):**
`SIGTERM`, `SIGINT`, `pm2 restart`/`pm2 stop` und das reguläre Herunterfahren
des Node-Linux-Systems lösen niemals selbst einen Drain aus. Daten, Placement
und Membership bleiben erhalten; erforderliche Zugriffe warten auf die Rückkehr,
wobei Reads erreichbare gültige Replikate bevorzugen. Unabhängige Zugriffe auf
andere Nodes laufen weiter. `DRAIN` darf ausschließlich durch einen expliziten,
autorisierten Admin-Command bzw. das Dashboard angefordert werden. Ein bereits
administrativ angeforderter Drain bleibt über einen Prozessneustart vorgemerkt
und wird danach fortgesetzt; das Stoppsignal selbst fordert keinen neuen an.

| Modus | Ablauf und Wirkung |
|---|---|
| `DRAIN` | Intern `DRAINING` setzen, Rebuild-Grund melden; nächster Plan evakuiert die Node. Nach sicherer Übergabe inklusive späterer Subscriptions `GOAWAY`, Verbindungen schließen, Membership-Leave abschließen. |
| `SHORT` | Wartungszustand `PAUSED` persistent ankündigen, neue lokale Mutationen anhalten, laufende Commits abschließen/flushen und herunterfahren. Placement/Membership/Koordinator bleiben erhalten. Peers und Clients warten bzw. verbinden erneut; kein Rebalance. Nach Start `RESUME_STATE` abgleichen und weiterarbeiten. |
| `CLUSTER` | Alle Mitglieder bestätigen eine gemeinsame Shutdown-ID, stoppen neue Mutationen, schließen laufende Commits ab, flushen Kontroll-/Datenzustand und melden `READY`. Erst nach allen READYs wird `STOP` verteilt und alle fahren im selben Shutdown-Lauf herunter. Keine Evakuierung und keine Membership-Änderung; bei teilweise zugestelltem STOP bleibt der Rest angehalten und verteilt STOP nach Reconnect erneut. |
| `FORCE_DISCONNECT` | Admin bestimmt ausgeschlossene Node(s), akzeptiert den Verlust aller ausschließlich dort verfügbaren Daten und bestätigt die neue Membership. Kein automatischer Timeout. Verfügbare gültige Replikate werden nach dem neuen Plan ergänzt; nicht rekonstruierbare Ranges werden `LOST` und antworten 410 statt scheinbar leerem 404. |

Ein SHORT/CLUSTER-Shutdown kann bei ausstehenden Commits oder fehlenden Mitgliedern warten; bestehende Operationen werden nicht durch einen Timeout als sicher abgeschlossen behauptet. Netzwerk-/Hardwarefehler isolieren fehlerhafte lokale I/O, ersetzen aber keine Verlustentscheidung. Nach FORCE_DISCONNECT müssen ausgeschlossene Instanzen **extern gestoppt oder vom Client-/Peer-Netz gefenced** sein, bevor die Restseite schreibend weiterläuft: eine Nachricht an erreichbare Peers kann einen isolierten alten Primary nicht abschalten. Die Admin-Anfrage enthält die bestätigte Fencing-/Verlustfreigabe. In diesem Ausnahmeweg koordiniert die niedrigste verbleibende Node-ID die Kontrollentscheidung; eine ausgeschlossene bisherige Anlaufstelle wird dafür nicht benötigt. Wiederkehrer werden mit alter Membership-Epoch abgewiesen und dürfen nur kontrolliert neu beitreten.

---

## 4. Quorum, Koordinatorwahl und Generationenablauf

### 4.1 Persistenter Kontrollstand

**Scope-Regel:** Globale Membership-/Admin-Entscheidungen besitzen weiterhin eine gemeinsame Sequenz-/Hashkette und ihre ALL-Barriere. Rebuilds sind dagegen **an betroffene Range-Groups gebunden**. Disjunkte Scopes dürfen parallel auch verschiedene Koordinatoren haben; überlappende Scopes müssen sich ausschließen. Jeder Rebuild-Beleg bindet Scope-ID, RoundID, die vollständigen bisherigen Range-Grenzen/Owner-Epochs und die feste Teilnehmermenge. Seine Sequenz/Vorgängerkette gilt innerhalb dieses Scopes und darf nicht mit der globalen Admin-Sequenz verwechselt werden. Ein fehlender globaler Initiator oder Koordinationszeuge blockiert keinen sonst unabhängigen Online-Scope.

Die Vereinigung bestätigter disjunkter Patches ergibt die gemeinsame Placement-Map. Generation/Map-Revision sind dabei keine globale Totalordnung gleichzeitig laufender disjunkter Rebuilds; Schreibautorität folgt den bestätigten Übergaben und Owner-Epochs der jeweiligen Range. Ein Merge darf eine neuere Übergabe nie durch einen alten Gesamt-Snapshot überschreiben. Split/Merge beansprucht sämtliche betroffenen bisherigen Range-Groups einschließlich ihrer Grenzen, nicht bloß schon vorausgesetzte neue Teilbereiche. Die reservierten Rebuild-Wirelayouts erhalten bei Umsetzung einen explizit gebundenen Scope; die vorhandene globale Membership-/Admin-Decision bleibt davon getrennt.

```go
type RoundID struct { MembershipEpoch, Generation uint64; Initiator [16]byte; Attempt uint64 }
type ControlState struct {
    MembershipEpoch, CurrentGeneration uint64
    Round RoundID
    Coordinator [16]byte
    Phase uint8 // IDLE, ELECTING, COLLECTING, SEALED, PREPARED, MIGRATING, FINALIZING
    NeedRebuild bool
    SnapshotDigest, PlanDigest [32]byte
    DecisionSequence uint64
}
```

Der Zustand liegt in einem replaybaren Kontrolljournal im Node-Datenverzeichnis; kein flüchtiges Dashboard-Objekt. Jeder Snapshot-/Kontrollbeleg bindet Membership-Epoch, Ausgangsgeneration, RoundID und Hash der Daten. Zusätzliche Race-/Wahlzustände werden erst nach den Stresstests spezifiziert. Kontrollentscheidungen besitzen eine monotone Sequenz und den Hash ihres Vorgängers. Eine Node bestätigt nie widersprüchliche Entscheidungen derselben Sequenz.

Zum persistenten Rundenzustand gehören außerdem die gebundene Teilnehmermenge, eingefrorene Placement-Bereiche und die noch offenen Rebuild-Gründe samt Offline-Abhängigkeiten (§4.3). Das obige Strukturbeispiel und die vorläufigen Wire-Layouts sind dafür noch zu erweitern; `NeedRebuild` allein kann diese Informationen nicht ausdrücken.

Der eigene **Auftrag als Koordinator** wird getrennt vom eigenen Teilnehmer-Slot persistiert, und zwar vor der ersten entfernten Reservierung. Er bindet RoundID, Ausgangsstand, feste Teilnehmermenge und Änderungsart. Nach lokaler READY-Zusage werden der vorbereitete System-Manifest-Verweis und die öffentliche Ergebnisantwort gespeichert, niemals der rohe Passwort-Request. Ein Absturz vor diesem gespeicherten Vorbereitungsabschluss führt zur wiederholbaren Absage auf **allen** gebundenen Teilnehmern, auch solchen ohne bereits erteilte Reservierung; deren persistierte Absage verhindert das Wiederaufleben durch verspätete Nachrichten. Ein bereits committed Teilnehmer darf niemals abgesagt werden. Ab gespeicherter Vorbereitung wird derselbe Auftrag nach Wiederkehr zu Ende geführt. Nach ALL-APPLIED werden die entfernten Slots und zuletzt der eigene Slot freigegeben; der Koordinator vergisst den Auftrag erst nach allen dauerhaften Freigabebelegen. Abgeschlossene Teilnehmer behalten wiederholbar abrufbare Belege, auch wenn der Koordinator mit dieser Entscheidung aus der Membership ausscheidet. Eine spätere Journalbereinigung benötigt bestätigte Aufbewahrungs-Watermarks.

### 4.2 Auslöser und Push-Wahl: funktionaler Vertrag, Race-Algorithmus später

1. In IDLE passiert ohne Rebuild-Grund keine Migration. Hat ein Peer einen Grund und kennt keinen laufenden Rebuild, bietet er sich direkt per REBUILD_OFFER an und beginnt mit der Vorbereitung.
2. Peers senden ihre Statistik-/Zustandsfragmente selbständig an den bekannten Kandidaten. Bei konkurrierenden Kandidaten gilt die niedrigste Node-ID als gewünschte Präferenz; sobald ein Kandidat alle erforderlichen Fragmente besitzt und der Lauf als gebunden veröffentlicht ist, soll er nicht mehr abgewählt werden.
3. **Die konkrete Behandlung konkurrierender Angebote, überkreuzter Fragmente und der Bindungsgrenze wird noch nicht algorithmisch festgelegt.** Zuerst werden reproduzierbare Stresstests und Messungen durchgeführt, erst danach der Race-Algorithmus entworfen. Der zuvor vorgegebene exklusive Grant-/ABORT-Algorithmus ist gestrichen; dafür gibt es noch keinen bewiesenen Doppelkoordinator-Ausschluss.
4. Der Testaufbau variiert gleichzeitige Auslöser, Broadcast-/Statistikverzögerungen, Duplikate, unterbrochene und wiederkehrende Peers, Node-ID-Verteilungen und Clustergrößen. Gemessen werden Konfliktrate, Zeit bis eindeutiger Koordinatorbindung, Nachrichten-/Flushaufwand, Wiederholungen und konkurrierende Planversuche. Baseline-Abläufe/Instrumentierung dienen der Messung, sind kein vorweggenommenes finales Wahlprotokoll.
5. Unverändert gelten die Sicherheitsziele: kein widersprüchlicher aktivierter Plan, keine doppelte Schreibautorität, nach Bindung kein automatischer Koordinatorwechsel und bei Crash Warten/Wiederaufnahme. Die Implementierung darf diese Ziele erst nach Entwurf und Verifikation des gewählten Algorithmus als garantiert deklarieren.

**Erster Messbaustein:** `server/rebuild_race_test.go` simuliert Angebote, Präferenzwechsel, Statistikfragmente, Duplikate und Pause/Wiederkehr mit reproduzierbaren Seeds und den Codecs aus `client/rebuild.go`. Er zählt mögliche konkurrierende Bindungen, ohne einer Kandidatin tatsächlich Autorität zu geben. Die Scheduler-Ticks sind virtuelle Zeit; echte Peer-/Netzwerk-/Crash-Messungen stehen noch aus. Der Harness legt ausdrücklich noch keinen Bindungs-/Wahlalgorithmus fest. Messaufruf und Baseline-Befunde stehen in `docs/server.md`.

Broadcast ist Fan-out über Peer-Verbindungen, kein Netzwerk-Broadcast. Die unten reservierten Wahl-/Konfliktnachrichten sind vorläufige Katalogeinträge; ihre Feldfolgen können nach der Messphase geändert oder entfernt werden. ALL-Plan-/Cutover-Barrieren beziehen sich auf die gebundene Online-Teilnehmermenge aus §4.3; deren sichere Bindung gehört zum noch zu entwerfenden Wahlprotokoll.

### 4.3 Plan, Migration und Folgegründe

Der folgende Ablauf gilt **pro Scope**. „Maximal CURRENT/NEXT“ bedeutet einen aktuellen und einen vorbereiteten Stand je betroffenem Scope; mehrere disjunkte Scopes dürfen gleichzeitig einen solchen Übergang haben. Eine Node darf Teilnehmer mehrerer disjunkter Scopes sein. Sie hält dafür getrennte persistente Rundenzustände und Rebuild-Gründe statt eines einzigen clusterweiten Koordinator-Slots. Membership-/Admin-Commits und globale Policy-Freigaben bleiben nach §4.5 geordnet. Änderungen an einer PG müssen über freigegebene Zielpolicy-/PG-Versionen den jeweiligen Ranges eindeutig zugeordnet bleiben, bis auch zurückgestellte Bereiche umgestellt sind.

```text
IDLE -> OFFER/COLLECT -> SEALED -> PLAN -> PREPARE_ALL
     -> MIGRATING/CATCH_UP -> CUTOVER_ALL -> CLEANUP -> FINALIZE_ALL
     -> ausführbarer Rebuild-Grund ? nächste Runde beim bisherigen Koordinator : IDLE
```

- **Online-Teilnehmer und eingefrorenes Placement:** Eine Rebuild-Runde bindet vor dem Versiegeln des Snapshots eine explizite Menge erreichbarer, synchronisierter und teilnahmebereiter Nodes. `PAUSED`-/Offline-Nodes gehören nicht dazu. ALL bei Statistikaufnahme, Plan, Cutover und Finalize bedeutet alle Teilnehmer dieser gebundenen Runde. Die Cluster-Membership bleibt unverändert. Die Teilnehmermenge darf nach Bindung nicht aufgrund eines Verbindungsfehlers verkleinert werden; fällt ein Teilnehmer danach aus, wartet die Runde auf dessen Wiederkehr (§4.4).
- Der Koordinator plant deterministisch aus den vollständig eingesandten Snapshots der Teilnehmer. **Jede Range-Group mit einer Location auf einer nicht teilnehmenden Node wird vollständig unverändert von CURRENT nach NEXT übernommen**, einschließlich Range-Grenzen, Locations, Rollen, Layout und Ownership-Epoch. Das gilt auch für READ_CACHE-Locations. Keine Evakuierung, Ersatzplatzierung, Replica-Reduktion, Split-/Merge- oder Ownership-Änderung dieser Gruppen. Unabhängige, vollständig online befindliche Range-Groups dürfen umverteilt werden; Ziele müssen Teilnehmer sein und geeignete Online-Volumes besitzen. Offline-Volumes teilnehmender Nodes werden ebenfalls nicht umplatziert; betroffene Range-Groups bleiben eingefroren. Veraltete Kapazitätsbilder sind keine Quelle freier Zielkapazität.
- Physische Reservierungen werden beim PREPARE auf den Zielvolumes erneut geprüft, da Statistikwerte keine Kapazitätslocks sind. Der Plan soll **so wenig Nutzdaten wie möglich bewegen, idealerweise null**. Bestehende zulässige Zuordnungen bleiben erhalten; Splits/Merges dürfen rein logisch sein. Redundanz, garantierte Reserven und erforderliche Drain-Vorgaben sind harte Constraints. Nicht online ausführbare Drain-/Policy-Anpassungen bleiben ausstehend; eine teilweise Umsetzung darf nicht als vollständig erfüllt gemeldet werden.
- `PLAN_PREPARE` enthält vollständige NEXT-Map, bereits verbindlich freigegebene Zielpolicies, Teilnehmermenge, unverändert übernommene Bereiche, Reservierungen und Planhash. Jeder Teilnehmer validiert und persistiert diese, bevor er `PLAN_READY` pusht. Erst nach ALL-READY persistiert/verteilt der Koordinator `PLAN_COMMIT` samt Belegen; jeder Teilnehmer pusht `PLAN_APPLIED`. Bulk-Copy startet erst nach ALL-APPLIED. Der Rebuild ersetzt keine global erforderliche Policy-/Membership-Freigabe nach §4.5.
- Writes bleiben bis zum expliziten Range-Cutover beim bisherigen Owner. Der Source stoppt neue Mutationen kurz, flusht und fencet sich persistent, Ziel bestätigt Daten-/Dedupe-/Subscription-Stand. Alle Rundenteilnehmer persistieren denselben Ownership-Epoch-Wechsel (`CUTOVER_PREPARE/READY/COMMIT/APPLIED`). Erst nach ALL-APPLIED bedient das Ziel neue autoritative Operationen. Keine zeitlich ablaufende Lease ist nötig, weil der alte Owner selbst persistent stillgelegt wird. Ist ein erforderlicher Beteiligter inzwischen nicht erreichbar, wird gewartet oder FORCE_DISCONNECT mit externem Fencing verlangt.
- **Während eines aktiven Laufs:** Jeder neue Grund wird mit `REBUILD_NEEDED` an den bekannten Koordinator gepusht. Dieser setzt vor Bestätigung weiterer Kontrollfortschritte persistent `needrebuild=true`; Detailgründe können zusätzlich gesammelt werden. Der Sender behält seinen Grund bis er in einem Snapshot einer folgenden Runde berücksichtigt ist. Eine verlorene Push-Nachricht kann so nach Reconnect wiederholt werden.
- **Vorgemerkte Gründe:** Rebuild-Gründe werden persistent mit ihrem betroffenen Node-/Volume-/Range-/Policy-Umfang geführt; `needrebuild` ist deren Sammelindikator. Bloße Erfassung in einem Snapshot erledigt keinen wegen Offline-Ressourcen zurückgestellten Grund. Er bleibt bis zur erfolgreichen Umsetzung erhalten, ebenso auf dem Sender bis zur bestätigten Übernahme bzw. Erledigung. Offline-Abhängigkeiten dürfen weder einen ausführbaren Online-Rebuild blockieren noch eine endlose Folge wirkungsloser Generationen auslösen.
- Nach Cleanup werden alle FINALIZE-Belege der Runde persistiert. Gibt es ausführbare Folgegründe, beginnt der bisherige Koordinator unmittelbar die nächste Runde mit aktuellen Fragmenten. Sind nur Offline-Abhängigkeiten übrig, geht der Cluster mit persistent gesetztem `needrebuild` nach IDLE. Nach Wiederkehr und Zustandsabgleich einer betroffenen Node bzw. eines Volumes wird die Ausführbarkeit erneut geprüft und bei ausführbarem Grund eine neue Generation ausgelöst. Eine während der Runde wiederkehrende Node wird erst in einer Folgerunde Teilnehmer; der laufende Plan bleibt unverändert.
- Kontrollphasen dürfen Nachrichten bündeln und unabhängige Ranges parallel abwickeln. Persistenz-/ALL-Barrieren innerhalb der gebundenen Teilnehmermenge bleiben zwingend; Push statt Request/Response entfernt keine notwendigen Zustimmungen.

### 4.4 Crash und Wiederaufnahme

- Bei Koordinator-Crash warten die Peers. Sie halten RoundID, Snapshots und Plan-/Cutover-Belege vor; es gibt keine Crash-getriebene Neuwahl.
- Nach Reconnect pusht jeder Peer `RESUME_STATE`, insbesondere die bekannte Koordinatorrolle. Der wiederkehrende Koordinator rekonstruiert den gemeinsamen Stand aus Journal und Belegen und setzt denselben Lauf idempotent fort. Noch nicht zugestellte Fragmente/READYs/APPLIEDs werden erneut gepusht.
- Eine zuvor nicht teilnehmende Offline-Node übernimmt nach Reconnect die inzwischen committed Generationen und Belege in ihrer Reihenfolge, bevor sie an neuen Kontrollentscheidungen oder Rebuilds teilnimmt. Ihr unverändertes Placement und ihre Ownership-Epoch bleiben gültig; unabhängiges Objekt-I/O auf diesen unveränderten Zuständigkeiten verlangt keine globale Rebuild-Barriere. Zurückgestellte Rebuild-Gründe werden erneut gemeldet und können in einer Folgerunde umgesetzt werden.
- Bei Verlust der Koordinator-Node kann nur ein ausdrücklich administrativer Membership-Reset unter FORCE_DISCONNECT die blockierte Runde ersetzen. Bereits verbindliche Cutover-/Commitstände dürfen dabei nicht rückgängig gemacht werden. Widersprüche blockieren den Recovery-Pfad bis zur Klärung.

### 4.5 Membership und administrative Kontrolländerungen

- Join/Leave: Die bisherige Membership bestätigt den Übergang vollständig, beim Join bestätigt auch das neue Mitglied seinen initialen Zustand. Erst dann gilt Epoch e+1. Ein geplanter Leave endet erst nach Evakuierung.
- FORCE_DISCONNECT: Alle **verbleibenden** Mitglieder bestätigen eine neue Epoch einschließlich Ausschlussliste, Fencing-/Verlustfreigabe und letztem rekonstruierbarem Kontrollstand. Es gibt keinen Beleg des ausgeschlossenen Mitglieds; dies ist eine administrative Ausnahme unter akzeptiertem Datenverlust, kein normaler Quorum-Failover.
- Bucket-/User-/Quota-/Credential-Änderungen werden als geordnete `CONTROL_CHANGE` im selben Journal committed. Pro Kontrollsequenz genau ein Digest und ALL-READY/APPLIED. Eine globale administrative Kontrollrunde benötigt unabhängig von parallel laufenden Range-Scopes eine eindeutige Koordinatorzuordnung und die Zustimmung aller erforderlichen Mitglieder. Sie stimmt Policy-/Membership-Freigaben und Map-Checkpoints mit laufenden Scopes ab; deren Bulk-Copy allein erfordert keine globale Daten-I/O-Sperre. Die Race-Behandlung wird gemeinsam mit §4.2 nach den Stresstests entworfen. Nach Abschluss gibt der Koordinator den Slot mit FINALIZE frei, ohne Plan/Migration zu starten, sofern kein Rebuild-Grund vorliegt. Es gibt keine dauerhaft bevorzugte Admin-Master-Node. Lokales Volume-Add und lokale Statistikmessung benötigen diesen globalen Commitweg nicht.
- Berechtigungsentzug gilt nach Veröffentlichung der gemeinsamen Kontrollentscheidung; laufende bereits linearisiert autorisierte Operationen dürfen enden. Neue Operationen müssen den neuen Stand prüfen. Ausschlüsse brauchen das zusätzliche externe Fencing aus §3.5.

---

## 5. Buckets, User und Policies

### 5.1 Namespace und Hash

- Bucket hat eine unveränderliche **128-Bit-Bucket-ID**, einen vom Admin verwaltbaren Namen und einen Policy-Verweis.
- Native Objektadresse ist `(bucket_id, object_id[32])`; **256 Bit gelten per STQR-Definition als eindeutige Objekt-ID**. SDKs dürfen IDs aus binären Keys ableiten; identische Keys in unterschiedlichen Buckets sind unabhängig.
- Hashwert: `H = SHA-256(domain-separator || bucket-id || encoded-key)` mit eindeutig längenkodiertem Key.
- Frontends speichern Namen und fachliche Metadaten bei Bedarf selbst in normalen Objekten; das native Disk-/Wire-Format benötigt dafür keinen vollständigen Key.
- Standardmäßig besitzt jeder Bucket seinen **eigenen logischen 256-Bit-Hashraum** (initial eine Range ab 0), damit Policies und Lastbalancing unabhängig bleiben. Leere Buckets können intern Default-Maps teilen; nach außen bleibt die Semantik bucketbezogen.

### 5.2 Bucket-Policy

```yaml
bucket:
  id: "a3446c68-7593-4fda-a684-0da84b1e3737"
  name: "application-data"
  state: active
  storage_strategy: ssd  # hdd | ssd | ram | cache
  compression: raw  # raw | gzip | lz4 | xz
  placement_mode: count # count | everywhere; everywhere = eine Kopie auf jeder Mitgliedsnode
  redundancy:
    mode: replication  # single | replication | erasure
    replicas: 3         # bei everywhere implizit alle Mitglieder; kein Integer-unendlich
    ack: all            # v1: all; Quorum-Varianten erst mit vollständigem Protokoll
    ec_k: null
    ec_m: null
  read_acceleration:
    enabled: true
    cache_class: ramcache
    max_copies_per_hot_range: 4
    cache_bytes_budget: 8GiB
  limits:
    quota_accounting: logical # logical | physical; Volume-Admission immer physical
    metadata_reserve_ppm: 150000 # Schätzaufschlag für logische Zusicherungen
    guaranteed_quota_bytes: 2TiB # Zusicherung in quota_accounting-Einheiten
    absolute_quota_bytes: 10TiB  # harte Grenze in quota_accounting-Einheiten; unlimited erlaubt
  access:
    users:
      - id: "user-17"
        permissions: [read, write, append]
  policy_version: 12
```

**Nur Administratoren** dürfen Buckets erstellen, löschen, umbenennen oder konfigurieren, einschließlich Rechte, Quotas und Tier-/Redundanz-Policies. Normale Benutzer bekommen **explizite Einzel-Bucket-Freigaben**. Administrative Benutzer können ergänzend nur lesende Dashboard-Rechte besitzen; v1 kennt mindestens `ADMIN` und `USER`.

### 5.3 Policy-Änderungen

- Admin schreibt **Desired Policy** mit neuer `policy_version` und auslösendem Rebuild-Grund.
- Die `CURRENT Storage Policy` bleibt für existierende Daten maßgeblich, bis die nächste Generation erfolgreich umgesetzt ist. Rechte/Sperren (§4.5) und die gesonderte Quota-Admission (§5.5) folgen ihren eigenen Wirksamkeitsregeln.
- Während HDD→SSD, Replication→EC oder `copies=2→3` können `CURRENT` und `NEXT` unterschiedliche Layouts haben; Storage-Locations tragen eindeutig kodierte Layout-/Fragmentversionsinformationen.
- Die Änderung wird erst als **erfüllt** gemeldet, wenn alle notwendigen Payloads/Kopien/Fragmente gemäß Zielstrategie gesichert (HDD/SSD persistent, RAM auf allen erforderlichen lebenden Kopien) und die Cutovers veröffentlicht sind.
- Verfügbarkeits- und Abrechnungszusagen müssen zwischen `requested`, `migrating`, `fulfilled`, `degraded` unterscheiden.

### 5.4 Sicherheits- und Quota-Regeln

- Server prüft **jede** Request-Autorisierung unabhängig vom Client-SDK.
- Permission-Bits: `read` (GET/GET_RANGE/STAT, später SUBSCRIBE/UNSUBSCRIBE), `write` (PUT/PUT_RANGE/TRUNC/CAS), `append`, `delete`, optional `manage` nur Admin. APPEND darf mit Append-Recht ein fehlendes Objekt anlegen, ohne allgemeines Überschreibrecht zu vergeben.
- Admin-Befehle erfordern Admin-Rolle; Node-Befehle Cluster-Rolle.
- Volumes rechnen **immer physisch**, einschließlich Index, Journal, Alignment, Arenen, Checkpoints und GC-/Migrationsreserven. Bucket-Quotas können logisch oder physisch geführt werden; §5.5 definiert Admission und Reservierung.
- Credentials und `CLUSTER_TOKEN` sind nie Bestandteil der frei veröffentlichten Placement-Map.
- Bucket-Deletion zunächst tombstonen/sperren, dann sicher und asynchron Daten entfernen; versehentliche sofortige Löschung von Nutzdaten vermeiden.

### 5.5 Quotas, physische Kapazität und Zusicherungen

- **Volume-/RAM-Pool-Admission ist immer physisch.** Benötigt werden neue Payload-/Extent-Bytes, Metadaten/Index, Recordheader, Alignment, Append-Vorreservierung, Checkpoint-/Dedupe-Änderungen und temporärer Copy-on-Write-/Replikations-/GC-Bedarf. Auch bei logisch unveränderter Länge kann ein Overwrite Platz benötigen. Volle Volumes geben **507** zurück, selbst wenn die logische Bucket-Quota noch frei ist.
- Bucket-Accounting ist konfigurierbar: `logical` zählt die logische Objektlänge (ohne Mehrfachzählung der Replikate), `physical` die dem Bucket zurechenbaren physischen Bytes aller erforderlichen Disk-/RAM-Kopien inkl. Index-/Arena-Anteilen. Gemeinsam genutzte Metadaten werden deterministisch anteilig kontiert; freie Betriebsreserve ist keine Bucket-Nutzlast.
- `guaranteed_quota_bytes` G ist ein zugesichertes Kontingent, `absolute_quota_bytes` A dessen harte Nutzungsgrenze; A darf `unlimited` sein. Für endliches A gilt `0 <= G <= A`. Beim Einrichten/Erhöhen wird G physisch unter Beachtung von Klasse, Replica-Faktor und Failure Domains reserviert; ist das nicht möglich, wird die Änderung atomar mit **507** abgelehnt.
- Bei logischem Accounting wird die physische Zusicherung aus Payloadbedarf gemäß Policy plus konfiguriertem geschätztem Metadaten-/Alignment-/Arena-Aufschlag (`metadata_reserve_ppm`) abgeleitet. Mindestobjektzahl-/Tiny-Append-Profile können zusätzliche Reserven erfordern. Eine Schätzung ist keine mathematische Garantie für beliebig ungünstigen Metadatenbedarf: überschreitet der reale Bedarf die Reserve, wird Admission mit 507 abgewiesen und die unzureichende Zusicherung sichtbar gemeldet. Ein vorgegebenes Workloadprofil oder physisches Accounting ermöglicht belastbarere Zusicherungen.
- Ungenutzte **physische** Reserven anderer Buckets bleiben geschützt. HDD-Zusicherungen dürfen unter §7.4 auf SSD gedeckt sein; SSD-Zusicherungen niemals auf HDD. Kapazität wird je geeignetem Volume/Failure Domain geprüft, nicht allein als clusterweite Summe.
- Vor jeder Mutation werden logisches Wachstum und **alle neu benötigten physischen Bytes** reserviert. Ausstehende parallele Reservierungen zählen mit. Kein partieller Erfolg bei 507. Retry/Dedupe verhindert doppelte Kontierung; fehlgeschlagene Operationen geben Reservierungen frei. Überwrites müssen die alte Version bis zum sicheren Commit erhalten können.
- DELETE/TRUNC reduzieren logische Nutzung nach Commit, physische Nutzung erst nach tatsächlicher sicherer Freigabe. Geschützte G-Reserven werden dadurch nicht zu frei verbrauchbarem Fremdplatz. SSD-Fallback, Migration und GC dürfen sie ebenfalls nicht aufzehren.
- Die verteilte Vergabe trennt **physische Node-/Volume-Kontingente** und bei logischem Accounting zusätzliche **logische Wachstumskontingente pro Bucket/Primary**. Die Summe bereits genutzter und noch ausstehend verfügbarer logischer Kontingente darf A nicht überschreiten. Replikation verbraucht die logische Quota nur einmal, physische Kontingente dagegen auf jeder Kopie. Nodes reservieren lokal innerhalb beider Grenzen; Kontingentübertragung ist eine geordnete Kontrollentscheidung und Migration übergibt Restkontingente ohne doppelte Vergabe. Physische Reserven allein garantieren keine harte logische Obergrenze.

### 5.6 Kompression und Platzierungsmodus

- Bucket-Option **`compression`**: `raw`, `gzip`, `lz4`, `xz`; Standard `raw`. Das Volumeformat trägt das tatsächlich gewählte Verfahren je Objektversion, zusätzlich Block-/Extent-Metadaten zur unabhängigen Dekodierung (§9.8). Die native API liest/schreibt immer logische unkomprimierte Bytes.
- Änderungen des Verfahrens sind Storage-Policy-Änderungen für NEXT. Bei Migration werden Quellbytes dekodiert und gemäß Zielpolicy neu kodiert; `raw` bedeutet dabei gegebenenfalls Dekompression. Erst danach ist die Zielpolicy erfüllt. Reine Re-Kompression darf Objektversion, logische Länge, APPEND-Offsets oder Request-ID-Ergebnisse nicht ändern.
- **`placement_mode=count`** nutzt die konfigurierte Replikationsanzahl. **`placement_mode=everywhere`** bedeutet eine vollständige Kopie auf **jeder** aktiven Mitgliedsnode, einschließlich datenhaltender Nodes ohne sonstiges Bucket-Placement. Dies ist die explizite Darstellung von „replication=unendlich“, kein numerischer Überlauf/Sentinel in replicas.
- EVERYWHERE behält einen Primary zur Mutationsordnung; alle Nodes sind lesefähige Replikate. ALL-ACK umfasst alle aktiven erforderlichen Mitglieder. Join baut die neue Kopie vor ihrer verbindlichen Aktivierung auf, Leave entfernt sie erst mit dem Membership-Wechsel. Temporärer Ausfall entfernt sie nicht. Ein Join muss auf jeder neuen Node genügend passenden physischen Disk-/RAM-Platz reservieren oder wird mit 507 abgelehnt.
- EVERYWHERE ist mit HDD, SSD und RAM kombinierbar; CACHE bleibt reserviert für später. Jede Node braucht die passende Speicherklasse (HDD darf SSD nutzen). Das globale physische Accounting zählt alle Kopien; die logische Bucket-Nutzung weiterhin nur einmal. EC und EVERYWHERE werden nicht kombiniert.

### 5.7 System-Bucket und Accountdaten

**Katalog und Bootstrap (zunächst Spezifikation, noch kein Bucket-Management im aktuellen Volume-Auftrag):**

- Im replizierten System-Bucket liegt je Bucket ein eigenes Objekt mit dessen vollständiger versionierter Konfiguration. Accounts/Userliste liegen in weiteren normalen Systemobjekten; Passwortdaten darin sind gesalzene Verifier.
- Ein separates Bucketlisten-Objekt enthält ausschließlich die **sortierten 256-Bit-Objekthashes** der Bucket-Konfigurationsobjekte, keine eingebetteten Konfigurationen. Die Sortierung ist lexikographisch über die 32 Hashbytes; Duplikate sind ungültig.
- Der System-Bucket und der Einstiegspunkt des Bucketlisten-Objekts benötigen feste, versionsgebundene IDs, damit der Katalog ohne bereits geladenen Katalog gefunden werden kann. Beim neuen Cluster zuerst das Konfigurationsobjekt des System-Buckets schreiben, danach die initiale Liste mit dessen Hash veröffentlichen. Neue Bucket-Konfigurationen werden vor dem Eintrag in die Liste persistiert; der Katalogwechsel muss atomar und wiederanlauffähig sein. Die genaue Mehr-Objekt-Recovery wird vor Implementierung des Bucket-Managements präzisiert.
- Beim Wiederanlauf vorhandene Liste und referenzierte Konfigurationen validieren; ein fehlender/beschädigter Katalog eines bestehenden Clusters darf keinen stillen Neu-Bootstrap auslösen. Multinode übernimmt diese Objekte per EVERYWHERE-Replikation vor Aktivierung eines beitretenden Mitglieds.
- Das automatisch angelegte **Admin-Volume ist ein dynamisch wachsendes Datei-Volume**, keine fest begrenzte Systempartition. Es startet standardmäßig bei **1M** und hält unter anderem Accounts und später den Bucketkatalog. Die konfigurierte Größe ist nur eine Anfangsgröße; notwendiges Wachstum erfolgt automatisch, begrenzt durch den tatsächlich verfügbaren physischen Platz. Automatisches Dateiwachstum ist für Blockdevices nicht zulässig.


- Ein reservierter **persistenter System-Bucket** mit stabiler Bucket-ID hält Usernamen, Passwortverifier, zusätzliche Adminaccounts, Rollen und ACL-Objekte als normale STQR-Objekte. Platzierung ist **EVERYWHERE**, Standardstrategie HDD mit erlaubtem SSD-Fallback, Kompression raw. Er wird automatisch angelegt und darf nicht auf RAM/CACHE umgestellt oder von normalen Clients gelesen/verändert werden.
- Der erste Clusterstart erzeugt Cluster-Token und initiales Admin-Passwort und speichert beides im Klartext in der lokalen Config. Der erste Admin erhält zusätzlich einen verifierbasierten Accountdatensatz im System-Bucket; weitere User-/Admin-Passwörter werden ausschließlich als gesalzene Passwortverifier dort abgelegt, keine Klartextkopien dieser Passwörter in Node-Configs.
- Auth- und Adminhandler benutzen intern normale Objektoperationen auf diesen Systemobjekten, nicht eine zusätzliche Userdatenbank neben dem Store. Kontrollentscheidungen können Objektversionen/Digests referenzieren; sie ersetzen nicht den System-Bucket als autoritative Ablage. Ein gültiges lokales System-Replikat erlaubt Authentifizierung auch bei fehlendem Primary.
- Der Cluster-Token aus der lokalen Config ermöglicht Node-Authentifizierung vor Übernahme der System-Bucket-Kopie; weitere Userauthentifizierung wird erst nach synchronisierter Übernahme freigeschaltet. Jede beitretende Node muss den System-Bucket erhalten. Die Bootstrap-Kopie ist eine ausdrückliche Ausnahme von „neue Nodes erst im nächsten regulären Storage-Placement“; andere EVERYWHERE-Buckets müssen spätestens vor ihrer Aktivierung auf dem neuen Mitglied nachgezogen sein.
- Der Daemon erzeugt dafür automatisch ein eigenes **dynamisch wachsendes Admin-Dateivolume** in `VOLUMES_DIR/admin` und nimmt es sichtbar in Config/Stats auf. `system_volume_bytes` ist ausschließlich eine konfigurierbare Anfangsgröße, keine Obergrenze. Kein zusätzlicher init-/Format-Schritt. Wiederanlauf nutzt dessen bestehenden Header und überschreibt keine beschädigten Accountdaten blind; ein Blockdevice ersetzt dieses automatisch wachsende Datei-Volume nicht.
- Nach Accountänderung werden die neuen Systemobjekt-Versionen auf alle erforderlichen Kopien übernommen, bevor der gemeinsame neue Auth-/ACL-Stand aktiv wird. Hashing, Sperren und Credential-Rotation gelten unabhängig davon, dass das lokale Bootstrap-Secret bewusst im Klartext vorliegt.

---

### 5.8 Placement Groups und Volume-Filter

Eine **Placement Group (PG)** bezeichnet in STQR eine administrativ definierte Platzierungsmaske mit stabiler ID und einer Liste erlaubter Nodes. Pro aufgeführter Node kann die Auswahl ihrer Datenvolumes eingeschränkt werden. Ein Bucket kann eine PG referenzieren oder ohne PG auf allen geeigneten Nodes/Volumes platziert werden. Die Maske bestimmt zulässige Ziele; Speicherklasse, Redundanz, Failure Domains und Kapazitätsgrenzen gelten zusätzlich. Sie verlangt allein keine Kopie auf jeder aufgeführten Node.

Pro Node-Eintrag besitzt der Volume-Filter einen expliziten Modus und eine Liste stabiler Volume-IDs:

| Modus | Zulässige Volumes der aufgeführten Node |
|---|---|
| **Alle** (`all`, Standard) | Alle ansonsten geeigneten Datenvolumes; keine Listenfilterung. |
| **Blacklist** (`blacklist`) | Alle ansonsten geeigneten Datenvolumes außer den aufgeführten Volumes. |
| **Whitelist** (`whitelist`) | Ausschließlich die aufgeführten, ansonsten geeigneten Datenvolumes. |

Eine leere Blacklist schließt kein Volume aus; eine leere Whitelist erlaubt kein Volume. Neu hinzugefügte Volumes sind bei Alle und Blacklist grundsätzlich zugelassen, bei Whitelist erst nach expliziter Aufnahme. Bei Alle ist die Liste nicht wirksam. Kein Platzmangel oder Offline-Zustand erlaubt ein Ausweichen außerhalb der Maske. Änderungen folgen dem Desired-/Effective- und NEXT-Generationenmodell (§5.3); für Offline-Placement und vorgemerkte Rebuild-Gründe gilt §4.3. Der reservierte System-Bucket behält seine clusterweite EVERYWHERE-Platzierung.

**Systemdaten und Bootstrap:** Auch der System-Bucket referenziert eine PG. Seine EVERYWHERE-Policy bestimmt weiterhin eine Kopie auf jeder erforderlichen Node; die PG wählt dort das konkrete persistente Metadatenvolume per Whitelist stabiler Volume-IDs. Es gibt keine gesonderte Laufzeitregel „Bucket-ID system ⇒ physisches Admin-Volume“. Die lokale Volume-Konfiguration liefert nur den minimalen Bootstrap-Anker zum Lesen der PG- und Systemobjekte. Anschließend gelten die persistierten effektiven Masken. Ein fehlender Node-Eintrag, eine leere Whitelist oder ein widersprüchlicher Bootstrap-/PG-Stand führt zum Anhalten, niemals zum Ausweichen auf ein anderes Volume. Neue Mitglieder benötigen ihren Metadatenvolume-Eintrag vor Aktivierung der Systemkopie. Die clusterweite PG-Aktualisierung folgt denselben Kontroll-/Generationsbarrieren wie andere Platzierungsänderungen.

PG-Beschreibungen werden als normales Systemobjekt mit stabiler Group-ID, Name, Revision sowie `effective:{version,nodes}` und optional `desired:{version,nodes}` geführt. Ein Node-Eintrag enthält `node_id`, `volume_filter` und `volumes`. Die serverseitige Bucket-Policy speichert die optionale `placement_group`-ID. Objektclients benötigen die PG-Maske nicht; ihre bestehenden Objekt-/Policy-Response-Bytes werden dafür nicht still erweitert. Die administrative PG-Editor-/Nachrichtenoberfläche wird gesondert versioniert. Der Volume- und Quota-Auswahlpfad muss dieselbe effektive Maske benutzen.

**Dashboard:** Der Bucket bietet eine PG-Auswahl einschließlich „Keine Einschränkung“. Im PG-Editor erhält jeder Node-Eintrag ein Dropdown **„Alle / Blacklist / Whitelist“**. Nur bei **Blacklist** oder **Whitelist** erscheint die editierbare Volume-Liste mit **Hinzufügen** und **Entfernen**; bei **Alle** ist sie ausgeblendet. Volumes werden mit lesbarer Bezeichnung ausgewählt und über ihre stabile ID gespeichert. Bereits ausgewählte Offline-Volumes bleiben sichtbar und entfernbar; ihre Nichterreichbarkeit ändert den Filter nicht. Dies beschreibt den vorgesehenen PG-Ausbau, nicht bereits implementierte Dashboard-Funktionen.

## 6. Placement-Map: Binärmodell und Ranges

### 6.1 Hashraum und Startgrenzen

Der Hashraum umfasst alle `2^256` Werte von `0` bis einschließlich `2^256-1`. Die Map speichert **nur Range-Startwerte**; ein Eintrag gilt ab `start` bis unmittelbar vor dem nächsten höheren Startwert. Der letzte reicht implizit bis `max256`. **Kein 257-Bit-Endwert ist nötig.**

Lookup pro Bucket: binäre Suche nach dem **größten `start <= hash`**. Server halten jede veröffentlichte Map sortiert und unveränderlich als **zusammenhängenden RAM-Speicherbereich**; Header und Locations werden über Offsets adressiert. Ein neuer Buffer wird vollständig validiert und atomar publiziert, alte Reader halten ihre Referenz bis zum Ende. Clients halten eine **segmentierte partielle Map** nach §6.4.

### 6.2 Gewünschte flache Darstellung und normalisierte Darstellung

Der bevorzugte kompakte Transport lässt mehrere identische Starts zu:

```text
(bucket_id, from, mode/role, node_id [, fragment, policy_id, flags])

bucket A, 0000, PRIMARY,      node-1
bucket A, 0000, REPLICA,      node-2
bucket A, 0000, REPLICA,      node-3
bucket A, 8000, PRIMARY,      node-2
bucket A, 8000, READ_CACHE,   node-4
```

**Semantik:** Ein maximaler Block identischer `(bucket_id, from)` ist eine **Range-Group**. Der erste logische Gruppeneintrag definiert eine einzige, widerspruchsfreie `policy_id` und die `ownership_epoch`; sämtliche Locations besitzen eindeutige Rollen/Fragment-Indizes. Die Rolle `PRIMARY` ist genau einmal je writable Range aktiv. Für EC existiert eine eindeutige Koordinations-/Schreibautorität unabhängig von `DATA_0`.

Für effiziente lokale Zugriffe verwendet die interne unveränderliche Server-RAM-Map zwei Arrays im selben zusammenhängenden Speicherbereich. Das serialisierte Transportformat ist separat in §13.3 definiert und wird beim Laden in diese Darstellung dekodiert:

```text
RangeHeader[]:
    bucket_id, start_hash[32], policy_id, location_offset,
    location_count, ownership_epoch, state, runtime_id
Location[]:
    node_id[16], role, fragment_index, flags
```

Die flache Tripelansicht lässt sich aus diesen Arrays deterministisch rekonstruieren. Dadurch muss `policy_id` nicht in jeder Replica-Location wiederholt werden.

### 6.3 Größenordnung

Bei **1 PB** logischer Daten und **10 GB** je Range entstehen näherungsweise **100.000 Ranges**. Der einfache Basis-Eintrag `(start: 256 bit, node: 128 bit)` benötigt 48 Byte: **4,8 MB** pro Generation, **9,6 MB** bei zwei Generationen. Mit `(from, to, node)` wären es 80 Byte und **8 MB** bzw. **16 MB**. **Nicht 16 GB.** Replikate, Policies, Epochs und Buckets erhöhen die reale Größe; auch dann bleibt die Map für viele Deployments klein. Die genannten Werte sind Dezimalgrößen und lediglich Rohdaten-Schätzungen.

### 6.4 Adaptive partielle Client-Map

- v1 benötigt keinen Map-Agent. Ein Client startet mit leerer Map oder ausschließlich als **UNKNOWN** markierten Dummy-Bereichen und kennt mindestens eine Kontaktadresse. Für einen unbekannten Hash darf er optimistisch eine zufällige bekannte Node ansprechen.
- Bei falscher Zuständigkeit pusht diese Node einen authentifizierten **Map-Ausschnitt** (`MAP_PATCH`) und liefert 301 samt demselben Ausschnitt im Folgeheader. Die doppelte Übermittlung ist idempotent; eine zusätzliche Map-GET-Runde ist nicht nötig. Der Patch umfasst vollständige Range-Gruppen inklusive expliziter Segmentgrenzen, Policy, Locations, Membership-/Placement-Version und Ownership-Epoch.
- Der Client ersetzt nur den abgedeckten Bereich seiner segmentierten Map. Unbekannte Bereiche bleiben unbekannt; die letzte gelernte Range darf **nicht** versehentlich bis max256 gelten. `end_exclusive` ist im Patch optional mit einem separaten `to_end`-Bit für das Hashraumende kodiert. Für Split/Merge enthält der Patch die vollständige Ersatzpartition des angegebenen Intervalls.
- Innerhalb derselben Membership wird jeder überlappende Teilbereich separat zusammengeführt: Die höhere bestätigte `owner_epoch` gewinnt, unabhängig von der Map-Revision eines anderen Scopes. Bei unverändertem Owner, Runtime und Range-Grenzen kann eine höhere `policy_version` die Policy aktualisieren. Gleich alte widersprüchliche Angaben machen nur ihren Schnittbereich UNKNOWN; andere Teile desselben Patches werden weiterhin übernommen. `map_revision` ist ein Aktualitätshinweis bei sonst identischem Eintrag, keine globale Totalordnung disjunkter Rebuilds. Ein alter Gesamt-Snapshot darf weder eine neuere Übergabe zurückrollen noch neue Informationen für andere Bereiche blockieren. Die Client-Map lernt Routen; die serverseitige Autorität benötigt weiterhin validierte Übergabebelege. Eine neuere committed Membership verwirft alte Client-Segmente; Patches älterer Memberships werden ignoriert.
- Server können relevante Patches aktiv an verbundene Clients pushen. Ein explizites `MAP_GET` ist nur eine optionale Vorausladeoptimierung. Patches enthalten keine fremden Bucket-Rechte oder Secrets.

### 6.5 Splitting

- Nur **binäre Halbierung des Hashintervalls**, mathematisch exakt auf Bit-/Präfixgrenze.
- **Split allein verursacht keine Payload-Migration**; beide Tochter-Ranges können zunächst dieselbe Location-Gruppe besitzen.
- Vor und nach Split werden tatsächliche `live_bytes`, `objects`, `read_ops`, `write_ops`, `read_bytes`, `write_bytes` ermittelt.
- Split nach Kapazität oder Last; Größenheuristik z. B. `target=10 GiB`, `split_trigger=8 GiB`, mit Hysterese und Ausnahmen für Hotspots.
- Hashbereich ≠ Datenmenge. Eine Halbierung erzeugt nicht zwingend gleich viele Bytes; die Statistik muss tatsächlich gemessen bzw. aus lokalem Index abgeleitet werden.
- Ein einzelnes Hot-Object kann durch wiederholte Splits isoliert, aber nicht innerhalb desselben Objekts durch Range-Moves lastmäßig verteilt werden; dafür Read-Caches/Chunking.

### 6.6 Merging

- Benachbarte Ranges **KÖNNEN** zu einer Range zusammengeführt werden; für einen reinen binären Präfixbaum ist ein Merge zunächst nur bei kompatiblen Geschwister-Ranges möglich. Größere zusammenhängende Leerbereiche werden durch wiederholte Geschwister-Merges aufgebaut.
- **Mindestens benachbarte leere kompatible Bereiche** sollen soweit möglich zusammengeführt werden; ein Bereich ist erst „leer“, wenn keine Live-Daten, noch notwendigen Tombstones, Snapshots oder offenen Migratonszustände übrig sind.
- Merges sind eine **Generationsentscheidung**. Sie dürfen neue Data-Ownership und Kopierarbeit erfordern, wenn die zusammengefassten Ranges verschiedene Nodes haben.
- Für nichtleere Ranges wählt die Planung bevorzugt den Node, der bereits die meisten Bytes der späteren Range hält (**minimaler Umkopieraufwand**), sofern Kapazität, Failure Domains, Policy und I/O-Last es zulassen.
- Ein Merge und Split dürfen als logische Map-Änderungen vorbereitet werden, bevor eine spätere Migration nötig ist; physische Segmente müssen nicht sofort reorganisiert werden.

### 6.7 Range-Policy und Locations

| Rolle | Bedeutung | Durable? |
|---|---|---|
| `PRIMARY` | Schreibautorität/Koordinator einer replizierten Range | Ja, wenn auf Persistent Volume |
| `REPLICA` | Vollständige Kopie gemäß Bucket-Strategie | HDD/SSD: persistent; RAM: Prozesslebensdauer mit Replica-Restore |
| `EC_DATA_i` | Kodiertes Datenfragment `i` | Ja, im definierten EC-Profil |
| `EC_PARITY_i` | Paritätsfragment `i` | Ja, im definierten EC-Profil |
| `READ_CACHE` | Zusätzliche ausfallsichere oder flüchtige Lesekopie | **Nein**, zählt nicht als Redundanz |

Replikationsfaktor, EC-Parameter, Basis-Storage-Class und Read-Cache-Budget sind **Bucket-Policies**; die Range-Map beschreibt die **konkreten** Locations und Layout-Versionen. Falls Policies geändert werden, kann NEXT bereits ein neues Layout enthalten, während CURRENT noch maßgeblich ist.

---

## 7. Realtime-Statistiken, Balancer und Hotspots

### 7.1 Ein gemeinsamer Statistik-Snapshot

Generationen-Builder und Dashboard verwenden **dieselbe** Funktion `CollectStats(filter) StatsSnapshot` sowie denselben binären Codec. Kein zweiter Dashboard-Zählerpfad. Einzelne Zähler werden lockarm gesammelt; ein Snapshot trägt eine lokale monotone Sequenz und ein gemeinsames Messfenster, ist aber keine globale atomare Clusteraufnahme. Physische Platzreservierungen werden zusätzlich beim Plan-PREPARE geprüft.

```go
type ID [16]byte
type ObjectID [32]byte
type StatsFilter struct {
    Scope uint8                 // LOCAL=0, CLUSTER=1
    Nodes, Volumes, Buckets []ID // leer = alle erlaubten
    IncludeRanges bool
}
type IOStats struct {
    Ops [6]uint64      // READ, PUT, APPEND, DELETE, TRUNC, CAS: kumulativ seit BootID
    LogicalReadBytes, LogicalWriteBytes uint64
    PhysicalReadBytes, PhysicalWriteBytes uint64
    Errors, InFlight, Queued, Flushes, FlushNS uint64
    LatencyNS [6][3]uint64 // je Op p50,p95,p99 im WindowNS
    LatencySamples [6]uint64
}
type VolumeStats struct {
    ID ID
    Path string
    Class, State uint8
    CapacityPhysical, AllocatedPhysical, ReclaimablePhysical uint64
    PayloadPhysical, MetadataPhysical, ArenaUnusedPhysical uint64
    GuaranteedRemainingPhysical, InFlightReservedPhysical, OperatingReservePhysical uint64
    AdmissionFreePhysical uint64
    Health uint8
    IO IOStats
}
type RangeStats struct {
    Bucket ID
    Start ObjectID
    PrefixBits uint16
    Volume ID
    Role, State, ActualClass uint8
    RuntimeID ID
    OwnerEpoch, ObjectCount, LogicalLiveBytes uint64
    AllocatedPhysical, MetadataPhysical, ArenaUnusedPhysical uint64
    TombstonePhysical, MigrationPinnedPhysical uint64
    CacheBytes, CacheHits, CacheMisses uint64
    SubscriptionCount, HandoffPending uint64
    IO IOStats
}
type BucketStats struct {
    ID ID
    Accounting, Strategy uint8
    LogicalBytes, AttributedPhysical, ObjectCount uint64
    Guaranteed, Absolute uint64
    AbsoluteUnlimited bool
    GuaranteedReservedPhysical, CacheBytes uint64
    IO IOStats
}
type StatsSnapshot struct {
    Schema uint16
    Node, BootID ID
    Sequence, CapturedUnixNS, WindowNS uint64
    MembershipEpoch, Generation, MapRevision uint64
    NodeState uint8
    NeedRebuild bool
    ReasonBits uint64
    Coordinator ID
    Round RoundID
    CPUTimeNS, ResidentBytes, NetReadBytes, NetWriteBytes uint64
    IO IOStats
    Volumes []VolumeStats
    Buckets []BucketStats
    Ranges []RangeStats
}
```

**Zähleinheiten:** Alle Kapazitäten im Volume sind physische Bytes. `AllocatedPhysical` umfasst den ganzen belegten Platz einschließlich reclaimable, Metadata und ungenutzter Arena-Anteile; diese sind Teilmengen, nicht zusätzlich zu addieren. Freie physische Bytes sind `CapacityPhysical-AllocatedPhysical`. `AdmissionFreePhysical=max(0, frei-GuaranteedRemainingPhysical-InFlightReservedPhysical-OperatingReservePhysical)`. Die drei Reserven sind disjunkt; reservierte, aber bereits allozierte Arena-Bytes werden nicht ein zweites Mal abgezogen. Shared-Metadata/GC werden separat im Volume erfasst, auch wenn die Summe zugeordneter Range-Bytes kleiner ist.

Bucket- und Range-LogicalBytes in einem Node-Snapshot zählen lokale Kopien und dürfen beim Clusteraggregieren **nicht über Replikate aufsummiert** werden: logische Nutzung/Objektanzahl pro Range nur vom gültigen Primary übernehmen. Physische Nutzung über alle autoritativen/replizierten HDD-/SSD-/RAM-Locations summieren; Caches getrennt halten. Bei unbekanntem/fehlendem Primary wird der zuletzt bekannte Wert mit `STALE` angezeigt, nicht mit null ersetzt. Der Aggregator leitet STALE aus CapturedUnixNS/BootID und Verbindungszustand ab; STALE ist kein Ausschluss-/Failover-Signal.

### 7.1.1 Binärformat, Push und Fragmentierung

- Wire-Layouts sind **nicht** das Go-Speicherlayout: Felder werden in oben gezeigter Reihenfolge mit §13.2-Primitiven kodiert. `ID=16 Byte`, `ObjectID=32 Byte`; `uint64=LE-u64`, `uint16=LE-u16`, bool/u8 je ein Byte; string=`Text`; Slice=`Vec(T)` mit Anzahl-Varint, feste Arrays ohne Anzahl. RoundID ist in §13.3 definiert. `Schema=1`.
- `STATS_GET(filter)` liefert einmalig einen Snapshot. `STATS_SUBSCRIBE(filter, interval_ms=3000)` legt eine Push-Subscription an; der Daemon pusht sofort und danach im **3-Sekunden-Intervall** `STATS_SNAPSHOT`. Das Dashboard subscribiert einmal und rendert diese Binärdaten; es pollt nicht alle 3 Sekunden per HTTP. `STATS_UNSUBSCRIBE` beendet den Stream.
- Bei `Scope=CLUSTER` richtet der angesprochene Daemon entsprechende Peer-Subscriptions ein, aggregiert anhand Node/BootID/Sequence und leitet Einzelsnapshots samt Stale-Status an sein Dashboard weiter. Mehrere Abnehmer teilen einen Collector; Filtern/ACL erfolgt vor Auslieferung. Entfallende Peer-Snapshots bleiben als veraltet sichtbar, ohne Mitgliedschaftsänderung.
- Für die Wahl sendet jedes Mitglied **nach REBUILD_OFFER selbständig** dieselben Snapshot-Bytes als STATS_FRAGMENT an den bekannten Kandidaten. Zusätzliche Felder binden RoundID, Snapshot-Digest, PartIndex/PartCount und lokale pending Admin-/Volume-Zustände. Das Fragment ersetzt eine explizite Request-Runde; NodeState enthält insbesondere DRAINING/PAUSED.
- Große Snapshots werden nach dem Kodieren in maximal 1-MiB-Teile zerlegt. Digest und SnapshotSequence binden die gesamte Bytefolge. Erst nach allen validierten Teilen zählt das Wahlfragment als vollständig. Dashboard-Snapshot-Fragmente werden ebenfalls vollständig zusammengesetzt, bevor ein neuer Renderstand aktiviert wird.
- Kumulative I/O-Zähler gelten pro BootID. Raten ergeben sich aus Deltas/Zeit; neue BootID, fehlende Samples und Counter-Überlauf erzeugen keinen negativen Durchsatz. Per-Op-Latenzen stammen aus dem Messfenster; ohne Samples zeigt das Dashboard „—“. EWMA/Hysterese/Cooldown begrenzen Last-Rebuilds; Tochterrange-Schätzungen stammen aus Hash-Bins oder Index-Scans.


### 7.2 Balancing-Ziele

Priorität:

1. **Sicherheit:** vereinbarte Redundanz und Failure Domains erfüllen; gefährdete Ranges priorisiert wiederherstellen.
2. **Kapazität:** maximalen Volume-/Node-Füllstand reduzieren, reservierte Rebuild-Kapazität beachten.
3. **Migration minimieren:** vorhandene zulässige Eigentümer behalten; wenn die Constraints schon erfüllt sind, bevorzugt **0 Byte Migration**.
4. **Last:** notwendige Hotspot-Entlastung mit möglichst wenig Kopierarbeit; keine beliebigen kosmetischen Moves.
5. **Kosten/Policy:** HDD/SSD-Basis und Premium-RAM-Cache-Budgets einhalten.

Eine mögliche Zielfunktion lautet:

`J = w1 * risk + w2 * max_utilization + w3 * p99_latency + w4 * bytes_to_move + w5 * tier_cost`

Diese Zielfunktion ist ein Planungsmodell, keine zugesagte globale optimale Lösung. Für v1 genügt ein deterministischer Greedy-Plan mit nachvollziehbaren Kosten und Simulation vor Commit.

### 7.3 Read vs. Write

- **Hohe Read-Last** kann zusätzliche Lesekopien/Cache-Replikate, Read-Replica-Selection und Hot-Range-Splits rechtfertigen.
- **Hohe Write-/Append-Last** spricht **gegen** beliebig viele synchrone durable Replikate und nicht automatisch für Cache-Erweiterung. Der Balancer kann die betroffene Range zu einem weniger belasteten Primary verlagern oder große Workloads über unabhängige Ranges verteilen.
- Ein einzelnes Hot-Object bleibt ein einzelner Serialisierungspunkt für atomare Appends; auch beliebig viele Range-Splits ändern diese Grenze nicht.

### 7.4 Node-/Volume-Planung

- Globaler Planer wählt **Nodes** für Range-Gruppen, unter Einbezug physischer Volume-Kapazitäten und Reservierungen. `storage_strategy` wählt die Strategie: **HDD darf auf SSD ausweichen, SSD niemals auf HDD**; RAM ist ein eigener flüchtiger Pool, CACHE reserviert für später. Gibt es für SSD keinen geeigneten physischen Platz, folgt 507.
- Node-interner Volume-Manager wählt das konkrete geeignete Volume aus seinem zugesagten Kontingent unter Beachtung der PG-Maske und ihres Alle-/Blacklist-/Whitelist-Filters (§5.8). HDD-Fallback auf SSD ist nur zulässig, wenn die physischen Garantiereserven aller Buckets erhalten bleiben; insbesondere dürfen SSD-Zusicherungen nicht durch HDD-Fallback verbraucht werden. Die tatsächliche Klasse wird in Stats/Placement gemeldet.
- **RAMCACHE** darf nur `READ_CACHE` enthalten, niemals das autoritative Exemplar; autoritative RAM-Buckets verwenden den getrennten RAM-Pool.
- Ein neuer Node wird nicht automatisch mit bestehenden Daten befüllt: NEXT kann zunächst neue Ranges oder Wachstum zuweisen und echte Migration sparsam planen. Bei gleichmäßig verteilten Objekthashes können bereits belegte Ranges jedoch nicht ohne zusätzliche Indirektion „nur für neue Objekte“ auf neue Nodes geroutet werden.

---

## 8. Volumes, Klassen und Hotplug

### 8.1 Volume-Arten

- **Regular file:** ein mit STQR formatiertes File im normalen Dateisystem.
- **Raw blockdevice:** dieselbe STQR-On-Disk-Struktur direkt auf einem Blockdevice.
- **RAM-Pool:** autoritative prozessgebundene Daten von RAM-Buckets, kein On-Disk-Format.
- **RAMCACHE:** zusätzliche flüchtige Read-Kopie; kein normales persistentes Volume.

Persistente Volume-Klassen sind `hdd` und `ssd`; daneben verwaltet der Daemon flüchtige RAM-Pools. Die **Bucket-Strategie** ist davon getrennt:

| Strategie | Platzierung und Haltbarkeit | Umfang |
|---|---|---|
| `hdd` | HDD, bei Platzmangel SSD zulässig; ACK disk-durable gemäß Replikationspolicy | v1 |
| `ssd` | Nur SSD; niemals HDD-Fallback, sonst 507 | v1 |
| `ram` | Autoritative Daten in replizierbaren RAM-Pools; keine beliebige Verdrängung, jede Kopie lebt solange ihr Prozess. Lokaler Neustart leert die Kopie, lebende Peers restaurieren sie. | v1 |
| `cache` | Best-effort-Objekte; STQR darf jederzeit Objekte verlieren/verdrängen, auch ohne Neustart. | für spätere Version reserviert |

**RAM ist kein READ_CACHE:** Ein RAM-Bucket hat RAM als autoritativen Basisspeicher, z. B. für Spielezustand oder Anwendungscaches mit prozesslanger Lebensdauer. `READ_CACHE`/RAMCACHE bleibt eine zusätzliche entbehrliche Beschleunigung für andere Buckets und zählt nie zur zugesagten Redundanz. CACHE ist eine eigene Bucket-Semantik, kein Alias für zusätzliche Read-Replikate. In v1 wird die Aktivierung von CACHE mit 501 abgelehnt; es gibt keine stillschweigende Umdeutung zu RAM.

### 8.1.1 RAM-Buckets und Prozesslebensdauer

- Der Daemon besitzt einen konfigurierten `ram_bytes`-Pool neben `ramcache_bytes`. Dieser wird in Statistiken/Admission als RAM-Volume mit ID und tatsächlichen allokierten Speicherbytes einschließlich Index, Arena und Bufferreserven geführt, hat aber keinen persistenten Volumeheader. Ein Neustart baut einen leeren Pool auf; Bucketkonfiguration/ACL/Placement im Kontrolljournal bleiben erhalten.
- **RAM verwendet dieselbe Replikationspolicy wie HDD/SSD.** Bei `replicas>1` werden alle erforderlichen RAM-Kopien vor ACK aktualisiert; Persistenz besteht jeweils nur für deren Prozesslaufzeit. Nach Neustart einer Node ist ihre lokale Kopie leer und wird vor erneuter Aktivierung **von den noch lebenden gültigen RAM-Replikaten rekonstruiert**, einschließlich Versionen, Append-Offsets, Tombstones und Dedupe-Status. Sie darf nicht vor dem Restore leere/veraltete Daten als autoritativ ausliefern. Der ausgefallene Prozess wird weiterhin nicht automatisch aus der Membership entfernt.
- Während der Prozess lebt, gelten dieselben atomaren GET/PUT/Range/APPEND/TRUNC/CAS- und Dedupe-Regeln wie bei persistenten Objekten. Keine automatische Eviction: Bei ausgeschöpftem RAM-/Quota-Budget gibt es 507. Erfolgs-ACK bedeutet Aufnahme in den autoritativen RAM-Zustand, **keinen Disk-Flush oder Wiederanlauf-Durability**. SYNC_BARRIER ordnet RAM-Operationen, macht sie aber nicht persistent.
- Jede RAM-Range trägt eine gemeinsame `runtime_id` ihrer zusammengehörigen Replica-Versionen, **nicht** die BootID eines einzelnen Prozesses. Bei Wiederherstellung aus lebenden Replikaten bleibt diese ID erhalten. Erst wenn alle erforderlichen Mitglieder ihren Zustand gemeldet haben und keine gültige RAM-Kopie mehr vorhanden ist (bzw. nach explizit freigegebenem Verlust), beginnt eine neue leere Runtime per geordneter Kontrollentscheidung. Bei einer Einzelkopie genügt die eigene entsprechende Verlustfeststellung. Nach vollständigem Cluster-Neustart sind alle RAM-Inhalte leer; Bucket-/Placementkonfiguration bleibt erhalten.
- Clientrequests können runtime_id=0 für „noch unbekannt“ senden; nach Lernen müssen sie die Runtime-ID mitführen. Retries aus einer früheren, vollständig verlorenen Runtime geben 410/RUNTIME_RESET samt aktuellem Map-Patch zurück, statt eine alte Mutation erneut auszuführen. Bei 0 und unbekanntem ursprünglichem Outcome besteht keine Garantie über vollständigen RAM-Verlust hinweg. Es gibt keine diskpersistente RAM-Payload-/Dedupe-Kopie; ein noch lebendes Replikat ist die Recovery-Quelle.
- Ein **DRAIN** überträgt lebende RAM-Objekte und Dedupe an das Ziel, bevor der Source endet; die gemeinsame Runtime-ID bleibt erhalten. **SHORT** verliert nur die lokale RAM-Kopie, die nach Wiederkehr aus überlebenden Replikaten restauriert wird. Ein kompletter **CLUSTER**-Neustart verliert alle RAM-Inhalte. Während benötigte Peers fehlen oder Restore läuft, warten betroffene Operationen; erst nach Restore bzw. bestätigtem Runtime-Reset darf die Node wieder autoritativ antworten.
- Guaranteed-/Absolute-Quota verwenden dieselben Regeln, reservieren bei RAM aber physische RAM-Pool-Kapazität einschließlich Metadatenaufschlag. Disk-Platz ist kein Ersatz für RAM-Garantien. Ein HDD-/SSD-Bucket fällt niemals still auf RAM/CACHE zurück und umgekehrt. OOM des Prozesses ist ein Prozessausfall, keine erlaubte Eviction-Strategie für RAM.


### 8.2 Live Hinzufügen/Entfernen

1. `VOLUME_ADD` über Dashboard/HTTP/WebSocket validiert Pfad/Device und Größe. Fehlt ein gültiger STQR-Header, **formatiert der Daemon das ausdrücklich hinzugefügte Ziel automatisch**, legt Header/Index/Segmente an und speichert die Volume-Konfiguration; kein separater Format-Aufruf. Ein gültiger Header wird gemountet bzw. seine Cluster-/Volume-Zugehörigkeit geprüft. Bei einem bereits konfigurierten Volume mit beschädigtem Header erfolgt Recovery/Fehler statt automatischer Neuformatierung beim Neustart. Die Autorisierung zum Hinzufügen gilt für genau den benannten Pfad.
2. Neues persistentes Volume erscheint zunächst als `PENDING`; **erst in der nächsten Placement-Generation** wird es Ziel für neue Range-Placements.
3. `VOLUME_REMOVE` markiert `DRAINING`; **im nächsten Generationsplan** erfolgt die reguläre Entleerung. Ein physischer Defekt muss sofort aus dem I/O-Pfad genommen werden.
4. `UNMOUNT_COMPLETE`/`DETACH_COMPLETE` nur, wenn keine benötigten durable Kopien, laufenden Flushes oder autoritativen Ranges mehr auf dem Volume liegen.
5. Volumes können per Pfad/Dateiname unabhängig von Node-IDs konfiguriert werden. Bei Remount wird die Volume-ID aus dem Header geprüft, um Verwechslungen zu verhindern.

### 8.2.1 Anlegen im Dashboard und Library-Verwaltung

- **Größenwahl:** Der Slider verwendet feste diskrete Stufen `1M, 2M, …, 9M, 10M, 20M, …, 90M, 100M, 200M, …`, entsprechend auch K/G/T/P. K/M/G/T/P stehen für 1024er-Potenzen. Seine Position ist ein Stufenindex; Ziehen verändert niemals die Skala/Obergrenze. Texteingabe und Slider sind in beide Richtungen gebunden, das Größenfeld zeigt kompakte Einheiten. Auch eine gültige individuelle Texteingabe bekommt eine exakte Sliderposition. Minimum bleibt 256K; als letzter Punkt ist die exakte auf Volume-Seitengröße abgerundete Obergrenze erreichbar.
- Bei neuen Datei-Volumes ist die Obergrenze der aktuell für den Benutzer verfügbare Platz auf dem Dateisystem von `VOLUMES_DIR` (`Statfs.Bavail`, wie `df`, keine für root reservierten Blöcke). Bei RAM gilt `MemAvailable`; ein ausdrücklich positiv konfiguriertes RAM-Budget begrenzt zusätzlich den noch nicht vergebenen Anteil. `ram_bytes=0` nutzt automatisch den verfügbaren System-RAM. Der Daemon liefert diese Werte über `VOLUME_CAPACITY_GET` und prüft sie beim Anlegen erneut; Überschreitung gibt 507. Blockdevices nutzen stets die vollständige vorhandene Größe und haben keinen Größenregler. Bestehende Datei-Volumes sind ebenfalls nicht über diesen Regler veränderbar.

- **Kapazitätsänderungen:** Reguläre Dateien und RAM-Volumes können über die explizite Library-Verwaltungsoperation `Volume.Grow(sizeBytes)` wachsen; das Admin-Dateivolume verwendet dies automatisch bei Platzbedarf. Keine implizite Vergrößerung normaler bestehender Dateien beim Hinzufügen und kein Truncate/Grow für Raw-Blockdevices. Nach einer externen Vergrößerung (z. B. eines LVM-LV) liest `VOLUME_RESCAN` die aktuelle Kernel-Kapazität des bereits eingebundenen Devices erneut und übernimmt zusätzlichen Platz durch `Volume.Rescan()` ins Volume-Layout. Das Dashboard bietet für jedes aktive Volume **Vergrößern**. Datei- und RAM-Volumes verwenden die gleichen abgestuften, bidirektional gebundenen Größenregler: Minimum ist die bisherige Größe, Maximum **bisherige Größe + aktuell freier Platz** (Dateisystem bzw. RAM, zusätzlich begrenzt durch ein konfiguriertes RAM-Budget). Die Grenzen werden bei der Durchführung erneut geprüft. Blockdevices haben keinen Regler; sie übernehmen stets die volle aktuelle physische Device-Größe. Nur das Admin-Dateivolume wächst automatisch; normale Volumes wachsen ausschließlich auf ausdrückliche Anforderung. Es gibt keinen Schalter für automatisches Wachstum normaler Dateien. Sie ändert weder die LV-Größe selbst noch die Partitionstabelle; Schrumpfen wird abgewiesen. Neue Backend-Bereiche werden atomar in den Freiraumbaum aufgenommen; bestehende Objekte und Baumseiten werden beim Wachsen nicht vollständig kopiert. Ein größerer Backend-Umfang allein ändert beim Open noch nicht die committed Volume-Kapazität.

- Ein Admin darf im Dashboard jederzeit neue Volumes anlegen, einen **Namen** vergeben und die Größe per Schieberegler oder Eingabe aus Zahl und Einheit wählen, z. B. `50M`/`50m`. Die Umrechnung der UI-Eingabe in Bytes ist Aufgabe der späteren Dashboard-/Server-Schicht; die Volume-Library nimmt ausschließlich Bytegrößen entgegen. Speicherklasse HDD/SSD/RAM ist in der UI einstellbar; bei Blockdevices soll sie soweit möglich über Linux-Geräteinformationen automatisch vorbelegt und vom Admin korrigierbar sein.
- `VOLUMES_DIR` ist das Datenverzeichnis, Standard **`volumes/`** relativ zum Arbeitsverzeichnis. Es wird angelegt und vollständig in `.gitignore` ausgeschlossen. Go-Library-Quellcode liegt deshalb im separaten **`volume/`**-Package, nicht im ignorierten Verzeichnis.
- Datei-Volumes liegen unter `VOLUMES_DIR/<name>`. Zulässig sind einfache Dateinamen ohne Pfadtraversal; keine implizite Dateiendung. Existiert die Datei noch nicht, wird sie in gewünschter Größe angelegt und formatiert. Existierende Dateien werden nicht still vergrößert/verkleinert; gültige Header werden geöffnet, ein ausdrücklich hinzugefügtes unformatiertes Ziel kann gemäß §8.2 formatiert werden.
- Alternativ kann der Admin aus den Blockdevices des Systems auswählen: physische Disks/Partitionen und LVM-/Device-Mapper-Volumes. **Gemountete Devices dürfen nicht geöffnet/formatiert werden**, auch nicht über einen anderen Pfad/Alias. Identität und Mountprüfung erfolgen anhand Major/Minor und `/proc/self/mountinfo`, bei Disks auch für gemountete Partitionen und bei gemounteten abhängigen Device-Mapper-/LVM-Geräten. Das spätere Dashboard stellt die Auswahl bereit. Der Library-Konstruktor prüft für den tatsächlich übergebenen Devicepfad den Mount-/Exklusivzustand; es gibt keine UI oder CLI in der Library.
- Der Library-Struct `Volume` hält Volume-ID, Name, Backend/Klasse, Größe, Zustand, offenen I/O-/Recovery-Stand und physische Statistiken. Eine lokale Verwaltung unterstützt Add/Open, Pending/Active, Draining, Sync und kontrolliertes Close/Detach. Die Cluster-/Dashboard-Schicht entscheidet später über Placement und die sichere Übergabe; die Library nimmt diese nicht vorweg.
- UI-Größeneingabe: Suffixe K/M/G/T und k/m/g/t können ohne Beachtung der Groß-/Kleinschreibung eingegeben werden. Die Umrechnung ist kein Bestandteil der Volume-Library. Der Konstruktor erhält Dateiname, optional benötigte Neuanlagegröße **in Bytes** sowie HDD-/SSD-/RAM-Klasse. Vorhandene formatierte Volumes übernehmen ihre gespeicherten Werte; Raw-Devices behalten ihre physische Größe.

### 8.2.2 Geräte-/Dateiauflistung für die spätere Admin-API

`volume.ListBlockdevices()` liefert typisierte exportierte Structs für alle verwendbaren Gerätepfade (Disks, Partitionen, LVM/Device-Mapper) und ungeclaimte reguläre Dateien in `VOLUMES_DIR`. JSON-Marshalling erfolgt später in der Admin-Schicht; die Library liefert bereits JSON-Tags für `path`, `size_bytes`, `type`, `class`, `class_detected` und optionale `aliases`. Gemountete, als Swap/Loop genutzte oder bereits geclaimte Ziele werden ausgeschlossen. Der Konstruktor prüft den konkreten Pfad erneut; Linux-Blockdevices erhalten zusätzlich einen exklusiven Kernel-Claim. Größen in der Library sind immer Bytes, die Texteingabe bleibt reine UI-Aufgabe.

### 8.3 Memory Mapping – differenziert

**Anforderung:** effizienter, mmap-fähiger Volume-Zugriff, live zu- und abschaltbar. **Umsetzungsempfehlung:**

- **Placement-Maps:** zusammenhängender RAM-Buffer (§6.1); **Index-Checkpoints:** read-only `mmap`, wo sinnvoll.
- **Reguläre Volume-Dateien:** `mmap` für ausgewählte leselastige Bereiche *kann* eingesetzt werden; Schreibpfad nutzt kontrollierte `pwrite`/`pwritev`/`fdatasync` oder `io_uring`/geeignete Go-Integrationen, um Persistenz eindeutig zu steuern.
- **Raw-Blockdevices:** nicht davon ausgehen, dass sich jedes Device zuverlässig wie eine reguläre Datei `mmap`-en lässt; hier grundsätzlich `pread`/`pwrite` mit geeigneter Ausrichtung und Flush/FUA-Semantik. Die vereinheitlichte `VolumeIO`-API abstrahiert beides.
- `mmap` ist **kein Ersatz** für Durability, Journaling, `fsync`/Flush, Alignment oder sicheren Unmount. Truncation/Device-Verlust unter aktivem Mapping muss durch Lifecycle-/Referenzmanagement vermieden werden.

### 8.3.1 Software Contract der Volume-Library

- Ein Volume gehört exklusiv einer Node. Kein paralleles Mounten derselben Datei/des gleichen Devices durch mehrere Nodes/Prozesse oder zwei Instanzen; der Konstruktor nutzt zusätzlich eine exklusive lokale Sperre. Linux-gemountete Blockdevices sind ausgeschlossen. Der Vertrag wird in den Go-Kommentaren direkt am Volume-Typ und den Konstruktoren dokumentiert.
- `volume/Volume.go` stellt `Volume`, Konstruktoren und die lokale Verwaltung bereit. Dateiname, Bytegröße bei nötiger Neuanlage und Speicherklasse genügen. Ein fehlendes oder ausdrücklich geöffnetes unformatiertes Ziel wird automatisch angelegt/formatiert; ein gültiges Format wird geöffnet. Von einer bereits gültigen Volume-Identität erkannte Journal-/Payloadkorruption ist ein Recovery-Fehler, kein Anlass zum stillen Löschen.
- Das Backend ist I/O-bound. Parallele spätere WebSocket-Aufrufe dürfen pro Volume durch einen RWLock koordiniert werden: lesende Operationen gemeinsam, Mutationen/Format/Checkpoint/GC exklusiv. Es wird kein lockfreier oder global parallel schreibender Volume-Pfad verlangt.
- Der RWMutex bleibt privat. `Volume.Stat(key)` dient allgemeinen, eigenständigen Metadatenabfragen. Für zusammenhängende Zugriffe liefert `Volume.Open(write bool)` ein Handle: `false` hält den Read-Lock, `true` den exklusiven Write-Lock bis zu `Handle.Close()`. Der Aufrufer setzt sofort `defer handle.Close()`; `handle.Stat(key)`, `handle.ReadAt(...)` und beliebig viele `handle.CopyTo(key, offset, length, writer)` verwenden denselben gehaltenen Lock. `CopyTo` schließt das Handle weder bei Erfolg noch bei Fehler/Panic. Damit gehören Antwort-Metadaten und gestreamte Daten zu demselben geschützten Zustand. Ein Write-Handle bietet zusätzlich die Objektmutationen an; diese bleiben einzelne Commits. Währenddessen keine rekursiven Aufrufe der normalen Volume-Methoden oder Rückaufrufe aus Reader/Writer in Handle/Volume. Handle und Volume dürfen nicht kopiert werden.
- Die Library benötigt Go ≥ 1.24. `runtime.AddCleanup` registriert für vergessene Handles die Freigabe über einen getrennten, nicht auf das Handle zurückverweisenden Lease-Zustand. `Close` entfernt das Cleanup und gibt den Lock synchron genau einmal frei; paralleles Close wartet auf laufende Handle-Operationen. `runtime.KeepAlive` verhindert eine vorzeitige Freigabe während aktiver Operationen. Cleanup ist eine Absicherung, keine zeitliche/absolute Freigabegarantie der Go-Runtime; explizites `defer Close()` gehört zum Software Contract.
- Die Objekt-API ist ausschließlich am Handle angesiedelt; auf `Volume` gibt es keine zweite, automatisch sperrende Variante von GET/PUT/APPEND/TRUNC/CAS oder `CopyTo`. Read- und Write-Handles bieten `Stat`, `Get`, `GetRange`, `ReadAt`, `CopyTo`, `Extents`, `Scan`, `OperationStatus` und `ScanRequests`. Nur Write-Handles bieten zusätzlich die Mutationen (einschließlich Reader-Varianten), `Recompress`, `Restore`, `Repair`, `RestoreRequest`, `ForgetRequest` und `SyncThrough`. Ein Read-Handle weist diese mit `ErrReadOnly` ab. `Volume` behält die eigenständige allgemeine `Stat`-Abfrage sowie Volume-Verwaltung, Sync, Checkpoint, Compaction, Scrub und vollständigen Export/Import mit eigener Sperrverwaltung; diese Aufrufe finden außerhalb offener Handle-Sessions statt. `Handle.Close` beendet die Session, `Volume.Close` schließt das Backend.
- Payloads werden begrenzt/blockweise gelesen und geschrieben; bei Blockdevices bleiben physische Block-/Alignmentgrenzen gewahrt. Kein notwendiges Ganzobjekt-Mapping/-Buffering; mmap ist eine optionale spätere Optimierung.
- Index/Metadaten werden für schnellen Zugriff im RAM gecacht. Bei vielen kleinen Objekten wird ein **begrenzter Cache plus On-Volume-Index** verwendet; Lookup/Recovery dürfen keinen unbegrenzten Vollindex oder sämtliche Payloads in RAM verlangen. Der konkrete Library-Stand dokumentiert Cachegrenzen, Disk-Fallback, Checkpoint/Compaction und deren Tests.

### 8.4 VolumeIO

```go
type VolumeIO interface {
    ReadAt(p []byte, off int64) (int, error)
    WriteAt(p []byte, off int64) (int, error)
    Sync() error
    Size() (int64, error)
    Close() error
}
```

Ein `VolumeManager` führt separat Device-Fähigkeiten, Mindest-Alignment, Cache-/Flush-Fähigkeit, Class, SMART/Health und Lifecycle-Status.

---

## 9. Eigenes On-Disk-Format und Append-Arenen

### 9.1 Grundprinzip

Ein persistentes HDD-/SSD-Volume besitzt **Superblocks/Header, persistente Index-/Checkpoint-Daten und append-orientierte Payload-Segmente**. Kleine Objekte werden nicht als einzelne OS-Dateien angelegt. Der lokale Objektindex ist nach `(bucket_id, object_id)` geordnet; für Migrationen sind geordnete Range-Scans möglich.

```text
+----------------------+  Volume start
| Superblock A         |
| Superblock B         |  alternierende/generationierte Header
+----------------------+
| Checkpoint Directory |
+----------------------+
| Index Snapshots      |  ggf. mehrere Generationen
+----------------------+
| Append Data Segment  |  Objekt-Records, Deletes, Append-Records
| Append Data Segment  |
| ...                  |
+----------------------+
| Reclaimable Segments |  nach Compaction wiederverwendbar
+----------------------+
```

Dies ist die **logische** Struktur; Segmente und Checkpoints müssen nicht zwingend physisch zusammenhängende Bereiche bilden.

### 9.2 Superblock

- Magic `STQR`, Formatversion, Byte-Order, Cluster-ID, 128-Bit-Volume-ID, Node-ID (oder explizite Transfer-/Adoptionskennung), Storage-Class, physische Größe, Alignment, Checkpoint-/Log-Root, monotone Superblock-Generation, Checksummen.
- Redundante Superblocks mit prüfbarer Generation und CRC/Hash zur Crash-Recovery.
- Cluster-/Volume-ID verhindern unabsichtliche Reinitialisierung oder das gleichzeitige Mounten derselben Volume-Identität auf mehreren Nodes.

### 9.3 Record-Formate

```text
RecordHeader:
  record_type {PUT, PUT_RANGE, APPEND, TRUNC, CAS, DELETE, CHECKPOINT, ...}
  bucket_id
  object_hash
  object_sequence / append_sequence
  object_compression / codec_version / layout_version
  extent_kind {DATA, ZERO}
  logical_extent_length / stored_payload_length
  payload_length
  logical_offset / new_logical_length (opcode-spezifisch)
  flags
  header_checksum
Payload:
  bytes
RecordTrailer:
  payload_checksum
  commit/record-validation marker
```

V1 speichert bei `PUT`/`PUT_RANGE`/`TRUNC`/`CAS`/`DELETE` versionierte Records. Ein logisches Objekt kann beliebig viele Records/Extents umfassen; Record-, Segment- und Frame-Größen begrenzen nicht die logische Objektgröße. Ranged Updates und Truncation aktualisieren den Extent-Index, ohne das gesamte Objekt umzuschreiben. Bei `APPEND` kann ein Append-Record mit referenzierter Objektversion bzw. einem eigenen Log-Chain-Index verwendet werden, damit nicht das gesamte TX-Log neu geschrieben werden muss. Ein beschädigter oder unvollständiger End-Record darf nie als bestätigt gelten.

### 9.4 Indexierung

Der lokale Index bildet `(bucket_id, object_id)` auf `object_version`, Längen, Kompressions-/Layoutversion, DATA-/ZERO-Extent-Locations, ggf. Append-Arena, Tombstone und Checksumme ab.

- **Im RAM:** kompakter geordneter Index, optional Cache für sehr heiße Keys.
- **Persistent:** regelmäßige checkpointete Indexseiten plus replaybarer Record-/Metadaten-Log.
- Crash-Recovery lädt den jüngsten gültigen Checkpoint und spielt nur danach persistierte Records ab.
- Für HDD/SSD darf die Speicherung eines bestätigten Objekts nicht davon abhängen, dass der flüchtige In-Memory-Index den Crash überlebt. RAM-Indizes werden aus lebenden Replikaten restauriert bzw. folgen bei Totalverlust dem Runtime-Reset-Vertrag.
- Für sehr viele kleine Objekte müssen Hash-Tabellen vs. B+-Tree/Sorted-Table, Indexgröße, Wiederanlaufzeit, Bloom-Filters und Compaction mittels Benchmarks verglichen werden.

### 9.5 Garbage Collection

- Neue Versionen und Tombstones machen alte Records ungültig.
- Segment-Compaction kopiert nur noch lebende Records, aktualisiert Index atomar und gibt alte Segmente **erst nach durablem Checkpoint/Fencing** frei.
- Laufende Range-Migrationen, offene Leser, Snapshots (wenn später unterstützt) und Append-Ketten pinnen benötigte Records.
- Reserve für GC/Migration ist notwendig; **99 % Nutzdaten-Füllstand ist nicht automatisch sichere 99 % physische Auslastung**.

### 9.6 Persistenzpfad

Dieser Disk-Pfad gilt für HDD/SSD. RAM verwendet atomare speicherinterne Veröffentlichung auf allen erforderlichen Replikaten ohne Disk-Flush; dessen Laufzeit-/Recovery-Vertrag steht in §8.1.1.

1. Per Objekt autorisierte Operation ordnen und lokal eindeutig sequenzieren.
2. Vollständig prüfbaren Record appendieren; bei Replikation die erforderlichen Copy-/Commit-Schritte ausführen.
3. Daten und commitrelevante Metadaten dauerhaft flushen.
4. Flüchtigen Index veröffentlichen; durable Recovery-Pfad muss bereits garantiert sein.
5. Erst danach `SUCCESS` antworten.

**Group Commit:** parallele `PUT`/`PUT_RANGE`/`DELETE`/`APPEND`/`TRUNC`/`CAS`-Requests dürfen einen Flush teilen. Jede einzelne Operation erhält ihre Bestätigung erst nach ihrem eigenen dauerhaft wirksamen Commitpunkt. `fdatasync`/`fsync` bzw. Blockdevice-Flush werden nach geprüftem Persistenzmodell eingesetzt.

---

### 9.7 Append-Arenen und Extent-Blockliste

**ENTSCHEIDUNG:** indexierte Extent-Blockliste statt verketteter On-Disk-Liste. Der Objektindex referenziert eine checkpointbare Folge `(logical_start, physical_extent, capacity, used)`; Tail-Append und Range-Lookup benötigen kein Durchlaufen aller vorigen Blöcke.

- Reine PUT-Objekte belegen nur benötigte Daten-/Metadatenextents und erhalten keine vorsorgliche Append-Arena. Beim ersten APPEND (auch nach PUT) wird eine eigene Tail-Arena angelegt.
- v1 startet mit einer physisch reservierten 4-KiB-Arena bzw. mindestens einem Device-Alignment-Block. Wachstum erfolgt durch neue Arena-Extents in Blöcken, verdoppelnd bis 1 MiB; kein O(n)-Kopieren des vorhandenen Objekts. Reservierte unbenutzte Bytes zählen physisch, nicht als logische Länge.
- Viele 10-Byte-Appends mit jeweils Durable-Sync passen in dieselbe Arena. Ein bestätigter Präfix wird bei nachfolgenden Writes nie überschrieben. Daten stehen in noch unbenutzten Slots; ein journalisierter Descriptor publiziert Payload-Länge, Offset, Sequenz, Request-ID und Checksumme erst nach Flush. Falls das Device Writes auf denselben physischen Block nicht ausfallsicher isoliert, wird der betroffene Block copy-on-write oder über ein redo-fähiges physisches Journal aktualisiert. Vorreservierung ersetzt keine Torn-Write-Sicherung.
- Group Commit kann unabhängige Flushes bündeln; ein einzelnes explizit synchronisiertes Append bekommt sein ACK trotzdem erst nach persistenter Nutzlast **und** Descriptor/Dedupe. Sehr kleine synchrone Appends haben dadurch weiterhin Flush-/Journal-Kosten, aber keine neue OS-Datei oder vollständige Objektkopie.
- TRUNC/CAS/Range-PUT aktualisieren dieselbe Extent-Version atomar; freie Arena-Tails werden nur nach gesichertem Checkpoint zurückgegeben. Nicht zu Ende geschriebene Slots sind nach Crash keine sichtbaren Appends.

### 9.8 Sparse-Bereiche und Kompressionsblöcke

- Die Extent-Blockliste unterscheidet **DATA** und **ZERO**. Ein ZERO-Extent kodiert `(logical_start, logical_length)` ohne Nutzdatenbytes. GET/GET_RANGE erzeugt für diese Bereiche Nullbytes; logische Länge und logische Quota zählen den ganzen Bereich, physische Admission nur Extent-/Indexmetadaten. Gleichbedeutende ZERO-Nachbarn werden zusammengefasst.
- PUT_RANGE über EOF und verlängerndes TRUNC erzeugen ZERO-Lücken, ohne diese physisch mit Nullen auszuschreiben. Ein späterer Teilwrite spaltet den ZERO-Extent in erhaltene NULL-Bereiche und DATA-Extents. Recovery/Migration erhält diese Semantik; große Löcher werden nicht zur Kopie als Nullbyte-Stream materialisiert.
- Der Objektversionsheader enthält `compression` (`raw=0,gzip=1,lz4=2,xz=3`) und eine Layoutversion. DATA-Extents enthalten logischen Offset, unkomprimierte Länge, gespeicherte Länge, Codec-/Formatversion, physische Location und Prüfsumme. ZERO-Extents benötigen keinen Codec-Payload.
- Kompression erfolgt in unabhängig dekodierbaren Blöcken mit zunächst höchstens 1 MiB unkomprimierter Nutzlast pro Codec-Chunk innerhalb einer zusammenhängenden Objektallokation; die Objektgröße bleibt unbeschränkt. Ranged Reads dekodieren nur betroffene Blöcke. Ranged Writes aktualisieren RAW-Seiten an ihrer vorhandenen Adresse mit Redo-Sicherung. Bei komprimierten Daten werden nur betroffene Codec-Blöcke neu kodiert; passen sie in ihren vorhandenen Bereich, werden sie dort ersetzt, andernfalls wird ausschließlich dieser Block neu alloziert. Unberührte Payload-Blöcke bleiben an ihrer Adresse. Ein gzip-/xz-Gesamtstream über das gesamte Objekt wäre dafür ungeeignet und ist nicht das Objektlayout.
- Kleine Appends nutzen kleinere unabhängig kodierte Records innerhalb der vorreservierten Arena. Der gewählte Codec gilt auch dort; Compaction darf Records später zu größeren Blöcken zusammenfassen. Kein Überschreiben bestätigter komprimierter Präfixe. Codecwahl kann den Aufwand pro kleinem Sync-Append verändern, aber nicht den Persistenzvertrag.
- Migration dekodiert DATA-Extents an der Quelle bzw. am Ziel und kodiert sie in das Zielverfahren um. ZERO-Extents bleiben Löcher. Layout-/Codecwechsel ist eine physische Änderung, keine neue logische Mutation. Dedupe-Identität und Nutzdatenprüfungen beziehen sich auf die logischen Operationsbytes, damit Kompression keinen Retry verändert.
- Garantiereserven verwenden vorab keine garantierte Kompressionsquote: konservativ Rohdaten plus Codec-/Block-/Indexaufschläge ansetzen. Reale physische Nutzung misst gespeicherte Bytes inklusive dieser Metadaten. Die Codec-Frame-/Parameterprofile und genaue On-Disk-Kodierung werden vor Format-Freeze festgelegt.

### 9.9 Konkreter Library-Stand: kompaktes Format 2

Das überarbeitete `volume`-Format ist in [docs/volume-format.md](docs/volume-format.md) beschrieben. Es nutzt 4-KiB-B+-Trees für Objekt-/Requestindex und bytegenauen Freiraum, begrenzte Metadaten-/Baumseiten-Caches sowie ein Redo-Journal für Änderungen belegter Seiten. Es werden keine ganzen Indexverzeichnisse pro PUT kopiert. Elternseiten ändern sich nur bei geänderten Separatoren/Unterbaumwerten oder Splits/Merges. Die frühere zweite Bank und pauschale Halbvolume-Reserve entfallen.

Jedes Objekt besitzt ein Darstellungsflag: **CONTIGUOUS** für dicht gepackte PUT-Daten mit einer zusammenhängenden Allokation, **SEGMENTED** für APPEND mit Erweiterungslisten und vorreservierten Arenen. Der erste APPEND übernimmt vorhandene PUT-Daten ohne Kopie als erstes Segment. Ein vollständiges PUT setzt die Darstellung wieder auf CONTIGUOUS. RAW-PUTs werden nicht pauschal in 64-KiB-Extents zerlegt. Größere Objekte beginnen seitenaligned, belegen jedoch nur ihre tatsächlichen Bytes; kleine Objekte nutzen Restbereiche. Leere Objekte belegen ausschließlich Verwaltungsdaten. Jede freie Lücke bleibt erfasst.

PUT_RANGE und Range-CAS ersetzen bestehende RAW-Bereiche direkt mit Redo-Sicherung, statt das Objekt vollständig neu aufzubauen. Komprimierte PUT-Container werden bei Bedarf in einzeln adressierbare Codec-Chunks überführt; unveränderte Payloads bleiben liegen. Nur ein betroffener Chunk mit wachsender kodierter Länge muss umziehen. Große Teilwrites und deren Recovery nutzen begrenzte temporäre Spools für Redo-Bilder; keine historischen Objektkopien für Leser. Vollständiges PUT und TRUNC verwenden zunächst weiterhin ihren eigenen Neuaufbaupfad.

Gemeinsam belegte Seiten werden durch vollständige Redo-Seitenabbilder geschützt. Ein bestätigter Commit-Anchor verpflichtet Recovery zum Fertigstellen; vorher bleibt der alte Baum gültig. Defragmentierung kann überlappende Linksverschiebungen seitenweise mit persistentem Fortschrittszeiger ausführen. Grow/Rescan erweitern den Freiraumbaum. Shrink verkürzt Datei-/RAM-Volumes erst, wenn der gesamte entfernte Bereich frei ist; belegte Metadaten verhindern unsicheres Truncate. Es gibt keine Laufzeit-Kompatibilität zum verworfenen Vorabformat; bestehende Testdaten werden exportiert und neu importiert.

Bucket-/Userverwaltung, Single-Node-Objektzugriff und Dashboard verwenden diese Library. Multinode-Placement, garantierte Bucket-Reservierungen und weitere markierte Clusterfunktionen bleiben Server-Erweiterungen.

---

## 10. Objekt-API und atomisches APPEND für Journale und TX-Logs

### 10.1 Native API (v1)

```go
type ObjectStore interface {
    Get(ctx context.Context, object ObjectID) ([]byte, ObjectInfo, error)
    GetRange(ctx context.Context, object ObjectID, offset, length uint64) ([]byte, ObjectInfo, error)
    Put(ctx context.Context, object ObjectID, data []byte, opt PutOptions) (ObjectInfo, error)
    PutRange(ctx context.Context, object ObjectID, offset uint64, data []byte, opt PutOptions) (ObjectInfo, error)
    Delete(ctx context.Context, object ObjectID, opt DeleteOptions) error
    Append(ctx context.Context, object ObjectID, data []byte, opt AppendOptions) (AppendResult, error)
    Trunc(ctx context.Context, object ObjectID, length uint64, opt TruncOptions) (ObjectInfo, error)
    CAS(ctx context.Context, object ObjectID, offset uint64, expected, replacement []byte, opt CASOptions) (ObjectInfo, error)
    Stat(ctx context.Context, object ObjectID) (ObjectInfo, error)
}
```

Die Verbindung ist bereits im Konstruktor an genau eine Bucket-ID gebunden; einzelne Operationen erhalten keine Bucket-ID und bieten keine Bucket-/Node-/Adminverwaltung. Objekt-IDs werden direkt verwendet, ohne erneutes Hashen. `GET_RANGE`, `PUT_RANGE` und `TRUNC` gehören zur nativen Basis-API. Die dargestellten Byte-Slice-Methoden sind Komfortfunktionen für in den Arbeitsspeicher passende Transfers; vollständige und ranged Reads/Writes benötigen außerdem Streaming-Varianten mit `io.Reader`/`io.Writer`. Objektgröße und Transfergröße dürfen keine vollständige Pufferung des Objekts erzwingen. Endliche Integer-/Adressräume und explizite Frame-/Ressourcenlimits bleiben technische Grenzen, aber es gibt **kein internes festes Objektgrößenlimit**.

CAS gehört bereits zur Basis-API (§10.6). SUBSCRIBE/UNSUBSCRIBE sind für eine spätere Protokollversion reserviert (§10.7). Listing und sonstige Frontend-Metadatenindizes entstehen ausschließlich aus normalen Objekten in den jeweiligen Client-Libs. Die noch offenen API-Randfälle sind in §20.2 aufgeführt.

### 10.2 APPEND-Semantik – **zwingende Anforderung**

- `APPEND(bucket, key, bytes)` hängt Bytes **atomar und ungeteilt** am Ende genau eines logischen Objekts an. Existiert das Objekt noch nicht, legt APPEND es atomar an; der erste Offset ist 0. Bei einem vorhandenen leeren Objekt ist der erste Offset ebenfalls 0.
- Mehrere parallele Appends an dasselbe Objekt erhalten eine **totale Reihenfolge**; keine Byte-Interleavings, keine verlorenen Appends.
- Der Return-Wert enthält mindestens **Anfangsoffset, neue logische Länge und Objektsequenz/Commit-Index**.
- Nach `SUCCESS` sind die angehängten Bytes einschließlich notwendiger Metadaten entsprechend der Bucket-Strategie gesichert: HDD/SSD **dauerhaft**, RAM solange mindestens eine gültige Kopie in einem laufenden Prozess erhalten bleibt. Der Retry-/Runtime-Vertrag gilt gemäß §8.1.1.
- Ein `GET` nach bestätigtem Append von einer gültigen autoritativen oder Replica-Readquelle sieht den gesamten bestätigten Präfix; jeder Read-Cache muss dieselbe Konsistenzgarantie erfüllen oder darf dafür nicht verwendet werden.
- Mutationen einschließlich `APPEND` werden **pro Objekt am Primary atomar und total geordnet** ausgeführt. Replica-Reads sind nicht allgemein linearizierbar: Während einer laufenden Mutation dürfen verschiedene Replikate unterschiedliche lokal committed Versionen liefern; beim Replikawechsel sind dann auch Versionsrückschritte möglich. Liveness und lesbare Hot Objects haben Vorrang vor einer global synchronisierten Read-Sicht. Strengere Sichtbarkeit koordinieren Anwendungen bei Bedarf selbst, etwa durch MOSI mit Freigabe des M-Locks erst nach erfolgreichem Storage-Commit; Multi-Object-/TX-Transaktionen bleiben Aufgabe höherer Schichten.
- `APPEND` mit leerem Payload ändert ein vorhandenes Objekt nicht; bei fehlendem Objekt wird ein leeres Objekt angelegt. Es ist kein Ersatz für einen expliziten Sync-Barrier-Befehl.
- Optionaler `expected_length`/`expected_version` erlaubt bedingte Appends; Objektsequenz und version bezeichnen hier denselben monotonen Objektversionsstand; Konflikte geben `PRECONDITION_FAILED` zurück.
- Ein einzelner Append hält die Transport-/Request-Ressourcenlimits ein und kann über mehrere Frames gestreamt werden; lange Logs bestehen intern aus mehreren Append-Records bzw. Extents. Es gibt kein internes festes Limit der logischen Objektgröße; die Beispiel-Workloads mit 2–250 KiB und die 1-MiB-Objekte der Frontends begrenzen die native API nicht.

### 10.3 Unknown-Outcome und idempotentes Retry

Eine **Request-ID** ist eine vom Client erzeugte 128-Bit-Kennung für **eine konkrete Operation**. Sie ist weder Objekt-ID noch Verbindungs-ID. Antworten tragen diese ID zur Zuordnung paralleler Requests. Wiederholungen derselben Mutation nach einem Verbindungsfehler benutzen dieselbe ID; eine neue fachliche Operation bekommt eine neue ID.

Beispiel: `APPEND(log, "abc", id=X)` wurde gespeichert, aber das ACK ging verloren. Ein Retry mit X liefert das ursprüngliche Ergebnis/den ursprünglichen Offset statt "abc" ein zweites Mal anzuhängen. Ein weiterer beabsichtigter Append benutzt Y. Das ist der Grund für die Dedupe-Einträge; wie lange sie erhalten bleiben, ist noch offen.

Netzwerkfehler können auftreten, **nachdem ein Append dauerhaft geschrieben wurde, aber bevor dessen Antwort den Client erreicht**. Blindes Wiederholen würde TX-Log-Daten doppelt anfügen.

**Daher obligatorisch:**

```text
request_id = 128-bit client-generated unique id
APPEND(bucket,key,payload, request_id, [expected_version])
```

- Der Owner persistiert deduplizierbare Request-IDs zusammen mit dem Commit bzw. einem langlebigen deduplication-record; ein identisches Retry liefert **denselben Offset und dieselbe Sequenz** zurück, ohne doppelt anzuhängen.
- Dieselbe Request-ID mit anderem Payload/Key wird als Protokollfehler behandelt.
- Dedupe-Fenster/Retention ist **OFFEN**, aber muss für TX-Log-Recovery und Client-Retry ausreichend lang sein. Ein unbegrenztes Dedupe-Verzeichnis benötigt entsprechend geplante GC.
- Clients dürfen `TIMEOUT` nicht mit „fehlgeschlagen und nicht geschrieben“ gleichsetzen; sie müssen anhand der Request-ID wiederholen oder den Status abfragen können.

### 10.4 Vollständige und ranged Operationen, DELETE und TRUNC

- Ein vollständiges `PUT` ersetzt oder erzeugt ein einzelnes logisches Objekt ungeteilt (alte oder neue Version, niemals ein teilweise sichtbares Payload).
- `GET_RANGE(bucket, key, offset, length)` liest einen Bytebereich eines konsistenten Objektzustands. Die Antwort enthält den tatsächlich gelieferten Bereich, die logische Gesamtlänge und die Objektversion. Vollständige Reads verwenden Status 200, Bereichsreads 206.
- `PUT_RANGE(bucket, key, offset, bytes)` legt ein fehlendes Objekt an und überschreibt atomar den angegebenen Bytebereich. Die neue Länge ist max(alte Länge, offset+Payloadlänge); vorhandene Bytes außerhalb des Bereichs bleiben erhalten. Lücken bis offset sind logisch Null und werden als ZERO-Extents kodiert. Ranged Writes müssen kleinere Blöcke innerhalb eines größeren Objekts effizient aktualisieren können, ohne einen clientseitigen `GET`-Modify-`PUT`-Zyklus oder vollständiges Umschreiben des Objekts.
- `TRUNC(bucket, key, length)` setzt die logische Objektlänge atomar, **auch beim Vergrößern**. Neue Bereiche lesen sich als Null und werden als ZERO-Extents kodiert. Beim Verkürzen werden Bytes ab dieser Position logisch entfernt und später sicher freigegeben. Verhalten bei fehlendem Objekt siehe §20.2.
- Ein `DELETE` erzeugt einen zur Strategie passenden Tombstone (HDD/SSD persistent, RAM in allen erforderlichen RAM-Kopien), damit alte Kopien/Caches bei Migration/Failover das Objekt nicht wieder sichtbar machen.
- `PUT`, `PUT_RANGE`, `APPEND`, `TRUNC`, `CAS` und `DELETE` werden **gemeinsam pro Objekt serialisiert** und tragen eine Version/Sequenz. Reads sehen einen konsistenten Zustand; bei parallelen ranged Writes gibt es keine verlorenen Änderungen außerhalb des jeweils geschriebenen Bereichs.
- Alle Mutationen unterliegen demselben Durability-, Request-ID-/Dedupe-, Quota- und Placement-Vertrag. Dedupe muss auch nach Truncation, Löschen und Neuanlegen eindeutig bleiben; der genaue Lebenszyklus ist noch festzulegen (§20.1).

### 10.5 Durability vs. Redundanz

- **Single HDD/SSD:** Erfolg erst nach lokalem durablem Commit. Kein Schutz vor komplettem Device-/Node-Verlust.
- **RAM / SINGLE oder Replica N/ALL:** Erfolg nach atomarer Aufnahme auf allen erforderlichen RAM-Kopien. Neustart verliert nur die lokale Kopie; gültige überlebende Replikate stellen sie wieder her (§8.1.1). Erst Verlust aller Kopien leert die Runtime. Keine vorzeitige Eviction erlaubt.
- **CACHE (später):** Erfolg bedeutet nur best-effort Aufnahme, Verlust/Verdrängung jederzeit möglich; kein Durable-Vertrag.
- **Replica N / ALL:** Erfolg erst nach Commit auf allen erforderlichen Replica-Nodes gemäß Speichermodus; für HDD/SSD ist dieser dauerhaft. Bei fehlender erforderlicher Node warten neue abhängige Commits/Reconnects. Client-Cancellation beendet nur das Warten des Aufrufers; kein stilles Absenken des Replica-Faktors.
- **Mehrheits-ACK-Modi** werden erst implementiert, wenn Term-/Epoch-/Commit-Log-/Recovery-Semantik vollständig korrekt ist.
- **EC:** Erfolg erst nach den zur gewählten Kodierung erforderlichen dauerhaft gesicherten Fragmenten gemäß Policy und Crash-Recovery-Protokoll; kein nur im RAM gepuffertes ACK.

---

### 10.6 CAS für Blöcke und Frontend-Metadaten

`CAS(bucket, object_id, offset, expected, replacement, expected_version?)` vergleicht und ersetzt **denselben gleich langen Bytebereich atomar** unter der Objektserialisierung. Stimmen Vergleichsbytes oder optionale Objektversion nicht, folgt 412 ohne Änderung. Es findet kein ungeschütztes clientseitiges GET-Modify-PUT statt. v1 begrenzt einen CAS-Block auf 1 MiB; dies ist ein Operationslimit, kein Objektlimit.

Für die Neuanlage kann `Precondition.absent=true` mit Offset 0 und ohne expected_version verwendet werden; dann MUSS das Objekt fehlen und `expected` ist leer. Für den atomaren Ersatz eines ganzen Metadatenobjekts, einschließlich Längenänderung, gibt es `scope=OBJECT`: Für ein vorhandenes Objekt ist expected_version verpflichtend, neue Bytes ersetzen das gesamte Objekt. Für Neuanlage gilt stattdessen absent=true; absent und expected_version dürfen nicht zugleich gesetzt sein. Erfolgreiche Änderungen werden durable/dedupliziert wie PUT. CAS gehört zu v1 und ermöglicht optimistische Directory-/Manifest-Änderungen auf mehreren gleichzeitig gemounteten FUSE-Clients.

### 10.7 SUBSCRIBE und UNSUBSCRIBE (spätere Version)

- `SUBSCRIBE` ist ein Request mit Bucket/Objekt-ID, optionalem Bytebereich und Resume-Cursor. Zunächst liefert er wie GET/GET_RANGE einen konsistenten Snapshot samt Objektversion und Subscription-ID. Nachfolgende `OBJECT_UPDATE`-Pushes liefern unaufgefordert vollständige oder ranged Änderungen, APPEND-Bereiche, TRUNC-Längen und DELETE-Ereignisse, bis UNSUBSCRIBE erfolgt. Rechteprüfung entspricht GET; Änderungen an Berechtigungen beenden betroffene Subscriptions.
- Snapshotaufnahme und Aufnahme ins Update-Journal erfolgen an derselben Objektsequenzbarriere; Änderungen zwischen GET und Listener-Registrierung dürfen nicht verloren gehen. Jede Aktualisierung trägt `previous_version`, `new_version` und einen monotonen Subscription-Cursor. Ereignisse können nach Reconnect wiederholt eintreffen; Clients deduplizieren. Bei einer Lücke liefern sie keinen vermeintlich aktuellen Zustand, sondern fordern Snapshot/Resume an. Bei vielen Änderungen in einer Version werden alle Teilupdates als zusammengehöriger Satz publiziert.
- `UNSUBSCRIBE(subscription_id)` ist idempotent. Als Request bestätigt es die Beendigungsbarriere; bereits davor in die Verbindung geschriebene Updates können noch eintreffen und werden anhand der ID verworfen. FF-Unsubscribe liefert keine Abschlussbestätigung.
- Bei Migration übergibt der Source **nach den Objektdaten** auch Subscription-Deskriptoren, Berechtigungsbezug, letzte bestätigte Cursor und noch benötigte Update-Journalbereiche an das Ziel. Cutover-READY setzt deren Übernahme voraus. Ein TCP-/WebSocket-Socket wird nicht auf den anderen Host verschoben: der Source kann Updates zum bestehenden Client proxyen oder `SUBSCRIPTION_MOVED` mit neuem Owner/Resume-Cursor pushen. Das Ziel hält Replay-Daten bis zur bestätigten Wiederaufnahme bzw. expliziten Beendigung.
- Allein aktive Subscriptions dürfen beim Drain nicht still verloren gehen. Der Source bleibt bis zu gesichertem Handoff/Resume bzw. einem expliziten Resync-Vertrag als Relay vorhanden. Ein Short-Shutdown pausiert Subscriptions; nach Reconnect wird anhand des Cursors fortgesetzt. Journal-Retention/Backpressure sind in §20 noch zu verfeinern.
- Diese Commands und ihre Opcodes sind bereits reserviert; v1 ohne das Feature antwortet auf Requests mit 501. Das Plan-/Migrationsformat führt den Subscription-Handoff-Zähler bereits mit.

---

## 11. Replikation, Erasure Coding und Read-Replicas

### 11.1 Replikation für HDD, SSD und RAM

- Eine Range-Group enthält einen autoritativen `PRIMARY` und `N-1` `REPLICA`-Locations in geeigneten Failure Domains. Die jeweilige Strategie bestimmt Disk-Persistenz oder RAM-Laufzeit; die Replikationsanzahl und der ALL-Abschluss gelten für beide.
- Der Primary sequenziert `PUT`/`PUT_RANGE`/`DELETE`/`APPEND`/`TRUNC`/`CAS` und sendet die geordneten Operationen an Replikate.
- Replikate halten Replikationssequenz und Commitstand gemäß Strategie (Disk geflusht, RAM auf lebenden Replikaten). Bei fehlendem Primary wird gewartet; neue Autorität entsteht nur durch regulären Cutover oder administrativen Forceful Disconnect mit Fencing.
- v1 verwendet verbindlich `ALL`-ACK für alle erforderlichen Kopien; verringerte Verfügbarkeit bei Replikausfall wird dokumentiert.
- Beim Replica-Add wird `COPY -> CATCH_UP -> ACTIVATE` verwendet; alte Replik darf erst nach gesichertem Wechsel entfernt werden.

#### 11.1.1 ALL-Replikation

Der Ablauf gilt für alle v1-Strategien: HDD/SSD flushen Daten und Commitbelege auf Disk, RAM publiziert dieselben Sequenz-/Dedupe-/Commitinformationen auf den erforderlichen lebenden RAM-Kopien ohne Payload-Diskjournal. Bei RAM bedeutet Wiederherstellung den Abgleich mit überlebenden Peers, nicht die Wiedergewinnung aus lokalem Disk-Replay.

1. Primary serialisiert/prüft Precondition/CAS, reserviert physischen Platz und ordnet eine Range-Replikationssequenz zu. `REPLICA_PREPARE` verteilt dieselbe Mutation/Request-ID; große Payloads werden per Transfer gestreamt.
2. Jede erforderliche Replica inklusive Primary flusht Stagingdaten und Recovery-/Dedupe-Information und pusht READY. Unbestätigte vorbereitete Records sind nicht lesbar.
3. Nach ALL-READY schreibt der Primary seine Commitentscheidung und verteilt COMMIT mit den Belegen. Alle Replikate flushen Commitstand und pushen COMMITTED. Ein COMMIT darf nicht nachträglich ABORT werden; ABORT ist nur vor persistenter Commitentscheidung zulässig.
4. Jede Replica veröffentlicht nach lokal wirksamem COMMIT ihre neue Version atomar, bevor sie COMMITTED meldet. Bis dahin bleiben Reads auf ihrer vorherigen committed Version möglich; PREPARE oder das Warten auf andere Replikate errichtet keine Read-Sperre. Replica-Reads benötigen keine Primary-Validierung oder globale Read-Freigabebarriere und dürfen keinen nur vorbereiteten Stand bedienen. Erst nach ALL-COMMITTED publiziert der Primary den Index-/Versionsstand und liefert den Q-Erfolg; zu diesem Zeitpunkt muss die bestätigte Version auf allen erforderlichen Replikaten lesbar sein. Eine spätere geordnete Mutation darf sie ersetzen. Bereits laufende Reads dürfen ihren zuvor aufgenommenen konsistenten Snapshot beenden.
5. Nach Crash werden vorbereitete/committete Sequenzen und Originalantworten abgeglichen. Unbestätigte Operationen dürfen ein unknown Outcome haben; bestätigte niemals verschwinden oder doppelt wirken. Fehlt eine erforderliche Replica, wartet der Abschluss bis Reconnect oder explizitem administrativem Membership-/Policywechsel. Ein solcher Wechsel kann keine verlorenen Bytes rekonstruieren.

### 11.2 Erasure Coding (Roadmap)

- Policy z. B. `EC(k=4,m=2)`; Range-Group ordnet `DATA_i`/`PARITY_i` zu konkreten Nodes/Volumes zu.
- Bei 2–250-KiB-Beispielobjekten kann **per-Objekt-EC** zu viel Fragment-/RPC-Overhead verursachen. Bevorzugt wird eine zu evaluierende **segment-/stripebasierte EC-Schicht**.
- Für kleine synchrone Appends benötigt EC eine besondere Lösung (z. B. repliziertes durable Write-Ahead-Log + spätere EC-Kompaktierung), andernfalls sehr hohe Write-Amplification und aufwendige Teilstripe-Updates.
- Sicherheitsregel: Eine bestätigte Mutation muss auch vor der EC-Konvertierung ausreichend dauerhaft vorliegen. Ein lokales, unrepiziertes Staging-Log erfüllt keine HA-Durability.
- EC-Profilwechsel sind Datenkonvertierungen und werden über NEXT-Generation mit gemischten vorübergehenden Layouts abgewickelt.

### 11.3 Read Caches und Hotspot-Ranges

- Zusätzliche `READ_CACHE`-Locations sind keine Durable Replicas.
- `ramcache` ausschließlich für Reads, nie als Persistenz-ACK oder Recovery-Quelle.
- Hohe Read-Last kann Split, Selective Caching und mehrere Cache-Kopien auslösen. Hohe Write-/Append-Last soll nicht automatisch die Cache-Kopienzahl erhöhen.
- Ein Cache darf nur **nachweislich gültige Versionen** liefern. Für **immutable/versionierte Objekte** ist dies einfach; für mutable Objekte und TX-Logs sind Invalidation, Lease oder Owner-validierter Read erforderlich.
- **v1:** Mutierbare und appendbare Objekte werden standardmäßig **nicht** aus nicht kohärenten READ_CACHEs bedient; nur explizit unveränderliche Payloads oder serverseitig validierte Cache-Versionen sind cachefähig.
- Ein einzelnes Hot-Object kann durch Splits in eine sehr kleine Range isoliert werden, seine Reads werden dann durch zusätzliche Caches repliziert; Writes/Appends bleiben am autoritativen Owner sequenziert.

### 11.4 Tiering

- `hdd` oder `ssd`: dauerhafter Basis-Storage je Bucket; HDD darf SSD nutzen, SSD niemals HDD.
- `ram`: autoritativer replizierbarer Laufzeitspeicher ohne Eviction; `cache`: spätere Strategie mit beliebiger Verdrängung.
- `ramcache`: Premium-Read-Replica-Tier, Kapazität via Budget/Quota und Lastregeln.
- HDD-/SSD-basierte Cache-Locations können später optional ergänzt werden; die Bucket-Strategien HDD/SSD/RAM und die reservierte CACHE-Strategie bleiben davon getrennt.

---

## 12. Migration, Ownership und sichere Generationenwechsel

### 12.1 Pro Range zulässiger Zustand

```text
STABLE -> PREPARED -> COPYING -> CATCHING_UP -> CUTOVER -> CLEANUP -> STABLE
```

**PREPARED:** NEXT-Map von allen gebundenen Online-Teilnehmern der Rebuild-Runde committed und angewendet. Nicht teilnehmende Nodes behalten unverändertes Placement und ziehen die Map nach Reconnect nach (§4.3/§4.4). Alter Owner ist autoritativ.

**COPYING:** Snapshot der Range auf Ziel kopieren. Source verarbeitet weiter Writes/Appends und führt ein der Strategie entsprechendes Änderungsjournal (HDD/SSD persistent, RAM repliziert im Arbeitsspeicher).

**CATCHING_UP:** Ziel holt Änderungen, Deletes, TRUNC/CAS, Append-Sequenzen und Dedupe-Status vollständig nach; anschließend werden aktive Subscriptions und ihre Replay-Cursor übertragen (§10.7).

**CUTOVER:** Persistentes Selbst-Fencing des alten Owners, Ziel bestätigt synchronisierten Daten-/Dedupe-/Subscription-Stand; Ownership-Epoch-Wechsel mit Zustimmung aller gebundenen Rundenteilnehmer nach §4.3. Eingefrorene Range-Groups erhalten keinen Cutover.

**CLEANUP:** Neue Owner bedienen Requests; alte Daten erst nach gesichertem Cutover und ohne benötigte Referenzen entfernen.

### 12.2 Routing, ACK-Vertrag und Reads/Writes während des Moves

- Der Server prüft bei **jedem** Objektrequest Bucket, native Objekt-ID, zuständige Range, wirksame Placement-Version und Ownership-Epoch. Ein Request außerhalb der eigenen gültigen Zuständigkeit darf nicht allein aufgrund lokal noch vorhandener Bytes erfolgreich beantwortet werden.
- Mutationen laufen bis Cutover über den alten autoritativen Owner und danach über den neuen. **Bei fehlendem/pausiertem Primary dürfen erreichbare gültige Replikate bereits committed Versionen lesen und ausliefern**; Reads warten nicht allein wegen Primary-Ausfall. Ein RAM-Replikat im Restore und alte gefencete Kopien sind keine gültigen Readquellen. Schreibautorität wird dadurch nicht neu vergeben. Bereits laufende Operationen dürfen kontrolliert auslaufen, müssen aber in den synchronisierten Cutover-Commitstand eingehen. Fencing und Cutover dürfen keine nachträglichen Writes des alten Owners außerhalb dieses Stands zulassen.
- Routet ein Client aufgrund falscher oder veralteter Informationen an den alten oder einen unzuständigen Node, antwortet dieser mit **301/302** und einem variablen Folgeheader mit Bucket/Range, aktueller bekannter Map-Version, Ownership-Epoch sowie zuständiger Node-ID und Kontaktadresse(n). `301` bezeichnet die committed Neuzuordnung; `302` eine vorübergehende Weiterleitung zu einem aktuell zulässigen Ziel. Ein bloß vorbereitetes NEXT-Ziel ist noch kein gültiger autoritativer Owner.
- Routing-Hinweise werden nur an berechtigte Clients ausgegeben. Kann ein Node die aktuelle Zuständigkeit nicht verlässlich ermitteln, antwortet er mit `503`/`STALE_MAP` und bekannten Map-Kontaktstellen, statt einen ungeprüften Owner zu behaupten. Ist die alte Node bereits offline, lädt das SDK nach Verbindungsfehlern die Map über andere bekannte Clusterkontakte neu.
- Bei nicht erreichbarem Primary kann das SDK eine im aktuellen Replica-Set enthaltene erreichbare Kopie für GET/GET_RANGE/STAT wählen. Eine gültige Replica liest auch ohne vorherigen Failure-Timeout; ein Transportfehler gibt ihr aber niemals Schreibrechte. Der Client übernimmt den Routing-Hinweis bzw. aktualisiert die Map und wiederholt Mutationen mit **derselben Request-ID**. Redirects erzeugen keinen mutierenden Effekt. Redirect-Schleifen werden begrenzt und lösen einen Map-Refresh aus.
- **Ein Erfolgs-ACK garantiert die Einhaltung des zum Commit wirksamen Placements und der zugehörigen Policy**, einschließlich gültiger Schreibautorität, erforderlicher Kopien, tatsächlicher Speicherstrategie und Commitstand. HDD/SSD sind durable; RAM ist ausdrücklich an die Lebensdauer seiner Prozesse/Replikate gebunden und CACHE später verlustbehaftet. Eine veraltete Client-Map schwächt diese Zusage nicht. Bei einem Retry bestätigt eine gespeicherte Erfolgsantwort den ursprünglichen Commit; spätere Placement-Übergänge müssen dessen Daten entsprechend dem vereinbarten Disk-/RAM-Lebensdauervertrag erhalten.
- Ein drainender Node darf noch vorhandene Daten ausliefern, solange er für diesen Read weiterhin autorisiert ist und deren Aktualität nachweisen kann: vor Cutover als Owner, danach nur als weiterhin gültige Lesekopie bzw. mit Owner-validierter Version. Bloße Existenz alter Bytes reicht nach Update/Delete/TRUNC nicht. Andernfalls antwortet er mit Redirect.
- Alte Primaries werden gefenced und dürfen nach Cutover keine neuen Schreib-ACKs für die alte Autorität ausstellen. Für Read-Caches gilt die separate Validierungssemantik. Nach vollständigem Drain erfolgt `GOAWAY` auf allen Client-Verbindungen (§3.3).

### 12.3 Split/Merge mit Replica-Sets

- Logischer Split kann die bisherigen Replica-Locations für beide Kinder übernehmen: **0 Byte Migration**.
- Beim Merge kompatibler Ranges mit identischem Replica-Set: meist nur Map-/Index-Änderung.
- Bei unterschiedlichen Sets: Planner wählt Ziel-Location-Set unter Wahrung der Failure Domains; Byte-Kosten, Kapazität und Read-/Write-Last entscheiden; in NEXT werden Source und Target getrennt modelliert.
- Globale Generationen können viele unabhängige Ranges parallel migrieren, mit I/O-Limits und Recovery-Priorität.

### 12.4 Crash-/Failure-Fälle

| Fehler | Erforderliches Verhalten |
|---|---|
| Source-Crash vor Cutover | Warten und nach Wiederanlauf fortsetzen; keine automatische Ablösung, ausschließlich administrativer Verlust-/Fencing-Pfad bei permanentem Ausfall |
| Target-Crash beim Copy | Unvollständige Zielkopie verwerfen oder validiert fortsetzen; Source bleibt Owner |
| Koordinator-Crash | Warten, nach Reconnect Koordinatorrolle/Fragmente/Belege per RESUME_STATE mitteilen und denselben Lauf fortsetzen |
| Client mit alter Map | 301/302 mit gültigem Ziel, andernfalls 503/`STALE_MAP`; Refresh, idempotentes Retry |
| Ausfall unmittelbar beim Cutover | Persistente Fencing-/Decision-Belege entscheiden; auf erforderliche Peers warten, keine Epoch bloß aufgrund eines Timeouts erhöhen |
| Netzpartition | Keine neue Ownership/Membership durch Timeout; unveränderte unabhängige I/O kann weiterlaufen, abhängige Operationen warten; Forceful Disconnect erfordert administrative Verlustfreigabe und externes Fencing |
| Abmeldewunsch während Migration | Intern `DRAINING` vormerken, bei nächster Statistikabfrage melden, Entleerung im nächsten darauf aufbauenden Plan; keine unsichere sofortige Deletion |
| Lost ACK nach APPEND | Request-ID dedupliziert; kein doppeltes Log-Append |
| Bucket-Policy-Change parallel | NEXT wird committed; weitere Änderungen bis zur Folgegeneration in Queue |

---

## 13. HTTP, WebSocket und vollständiger v1-Nachrichtenkatalog

### 13.1 HTTP-Einstieg und Authentifizierung

Ein Go-`net/http`-Server im Daemon bedient Dashboard, Administration und das WebSocket-Upgrade unter derselben Adresse. **STQR wird ausschließlich im Rechenzentrums-Intranet ohne TLS betrieben**: HTTP und ws, keine Zertifikats-/CA-Einrichtung. Bibliotheken für HTTP/WebSocket übernehmen HTTP-Parsing und RFC-konformes Upgrade/Framing; STQR implementiert nur seinen Nachrichten-Codec. WebSocket-Subprotokoll: `stqr.v1`.

| HTTP-Pfad | Methode | Bedeutung |
|---|---|---|
| `/` und `/assets/*` | GET | Eingebettetes Dashboard, statische Assets ohne Secrets |
| `/api/session` | POST | HTTP Basic prüfen, geschützte Browser-Session ausstellen |
| `/api/session` | DELETE | Browser-Session beenden |
| `/api/message` | POST | Genau eine binäre STQR-Request-Nachricht; binäre Antwort mit demselben Codec |
| `/api/volumes` | POST | JSON-Komfort-API für VOLUME_ADD, gleicher Handler |
| `/api/volumes/{id}` | DELETE | VOLUME_REMOVE, gleicher Handler |
| `/api/volumes` | GET | Lokaler Stats-Snapshot auf Volumes gefiltert |
| `/api/ws` | GET + Upgrade | Clients, Dashboard und Peers; Basic oder autorisierte Browser-Session |
| `/api/health` | GET | Authentifizierter lokaler Zustand, keine automatische Membership-Wirkung |

`POST /api/volumes` akzeptiert `{name, source, path, class, size_bytes}` plus `Idempotency-Key` (128-Bit-Hex), bildet exakt VOLUME_ADD ab und liefert `{operation_id,state,volume_id,status}`. Weitere Administration läuft vollständig über `/api/message` oder `/api/ws`; ein separates REST-Endpoint pro OpCode ist nicht erforderlich. HTTP-POST-Requests tragen dieselbe Request-ID wie ihre binäre Nachricht. HTTP-Redirects von `/api/message` werden nicht automatisch verfolgt: 301 wird mit binärem Map-Patch ausgewertet, Credentials werden nicht ungeprüft an andere Hosts weitergereicht.

- Client/Admin: HTTP Basic (`username:password`) über Intranet-HTTP. Erster Admin/Cluster-Token werden automatisch im Klartext in der Config erzeugt; weitere User-/Admin-Passwörter als gesalzene Verifier in den Objekten des EVERYWHERE-System-Buckets (§5.7). Rollen/Bucket-ACL werden serverseitig beim Verbindungsaufbau bestimmt. Native Clients übergeben ihre feste Bucket-ID im Upgrade-Header `X-STQR-Bucket`; der Server prüft dort Existenz und Grant und speichert den aktiven Bucket-Pointer sowie die effektiven Rechte in der WebSocket-Verbindung. Admin- und authentifizierte Cluster-Peer-Rolle werden ebenfalls dort gespeichert. Der Datenpfad verwendet diese gebundenen Capabilities ohne erneute User-/Grant-Lookups; ein Bucket-Wechsel innerhalb derselben Verbindung ist unzulässig. Account-/Grant-Änderungen und Session-Ende invalidieren betroffene Verbindungen vor Bestätigung der Kontrolländerung. HTTP-Einzelrequests werden weiterhin jeweils authentifiziert.
- Browser können für WebSocket nicht beliebige Authorization-Header setzen. Das Dashboard authentifiziert daher zuerst `/api/session` per Basic und verwendet danach ein `HttpOnly`, `SameSite=Strict`-Session-Cookie ohne Secure-Flag für den vorgesehenen HTTP-Betrieb. Upgrade/Mutationen prüfen Origin/Host und CSRF-Token; Credentials gehören weder in URLs noch in lokale Map-Patches. Session-Revocation wird auf bestehenden WebSockets durchgesetzt.
- Node-Rolle: HTTP Basic mit reserviertem Username `node-<node-id-als-32-Hexzeichen>` und Cluster-Token als Credential, gebunden an geprüfte Cluster-/Node-ID in NODE_HELLO. Die lokale Cluster-Konfiguration stellt das Cluster-Token bereit; Usernamen im reservierten Node-Namensraum sind für normale Accounts verboten. Langfristig individuelle Node-Credentials sind möglich. Ein Cluster-Token macht einen Node zu einem vertrauenswürdigen Clusterteilnehmer, nicht zu einem Endbenutzer.
- Authentifizierung erfolgt vor dem Upgrade; keine eigene AUTH_CHALLENGE-Sequenz mehr. Nach erfolgreichem Upgrade sind ausschließlich binäre Messages zulässig. Textnachrichten schließen die Verbindung mit einem Protokollfehler.

### 13.2 Primitive und gemeinsamer Envelope

**Byteordnung Little Endian.** `u8/u16/u32/u64` haben feste Breite. `V` ist ein kanonischer unsigned LEB128/uvarint, höchstens 10 Byte mit geprüftem u64-Überlauf; überlange Kodierungen werden abgewiesen. `ID=16 Byte`, `OID/Digest=32 Byte`, `Bool=u8` mit 0/1. `Blob=V(length)+bytes`, `Text=Blob` mit gültigem UTF-8, `Vec(T)=V(count)+T*`, `Opt(T)=Bool+[T]`. Längen/Counts werden vor Allocation gegen das verbleibende Message-/Ressourcenbudget geprüft. Keine impliziten Padding-Bytes.

```text
Message := version:u8=1 kind:u8 opcode:u16 id:ID body:bytes-to-message-end
kind := 0 FIRE_AND_FORGET | 1 REQUEST | 2 RESPONSE | 3 PUSH
ResponseBody := status:V follow_header:Blob result:opcode-specific
follow_header := Vec(TLV)
TLV := tag:V value:Blob
```

Eine **binäre WebSocket-Message ist exakt eine STQR-Message**; kein zusätzlicher TCP-Length-Prefix und kein Vertrauen in WebSocket-Frame-Grenzen. Die WebSocket-Library setzt fragmentierte Frames zur Message zusammen. v1 maximal 16 MiB pro Message, Bulk-Daten in <=1-MiB-Chunks. Kompression im WebSocket-Transport ist standardmäßig aus, um unnötige CPU-/Memory-Kosten und doppelte Kompression zu vermeiden.

- `id` ist bei REQUEST/FF/PUSH eine eindeutige Event-/Operations-ID, bei RESPONSE die ID des beantworteten Requests. Bei mutierenden Objektrequests ist sie zugleich die persistente Dedupe-ID und bleibt bei Redirect/Retry unverändert. Reuse mit anderer OpCode/Objektadresse/Parametern/Payload-Digest ist 400. Eine neue Session darf dieselbe Operation-ID zur Recovery wiederholen.
- **Q** in den Tabellen = Request mit genau einer terminalen Antwort. **F** = Fire-and-forget ohne RESPONSE. **P** = unaufgeforderter Push. `Q/F` darf der Sender wählen; F liefert keine Erfolgs-/Durability-Zusage an den Sender. Insbesondere Objektmutationen benötigen Q, wenn ein ACK verlangt wird. Fehler bei F können optional über EVENT gemeldet werden, sind aber keine verpflichtende Antwort.
- Requests blockieren nicht die Empfangsschleife: SDKs/Dashboard geben Future/Promise zurück und multiplexen anhand ID. Ein lebender Request bekommt eine terminale Antwort; bei Transportabbruch oder Client-Cancellation kann das Outcome unbekannt sein und muss mit derselben ID geklärt werden. Das ist keine automatische fehlgeschlagene Mutation und kein Membership-Timeout.
- Ein bekannter SHORT-/Offline-/Restore-Zustand eines weiterhin erforderlichen Peers erzeugt keinen impliziten Failure-Timeout: abhängige Requests bleiben ausstehend, solange Transport und aufrufender Context das erlauben. Ein freiwilliges Client-Deadline/Disconnect beendet nur diesen Aufruf. 503 darf fehlende sichere Routing-/Autoritätsinformation melden, ersetzt aber weder die Mitgliedschaftsentscheidung noch das vereinbarte Warten bei bekanntem Peer.
- F bedeutet „kein Warten auf Remote-Antwort“, nicht unbeschränkte Speicherbelegung. Senden erfolgt über eine begrenzte Queue; volle Queue gibt lokal `WOULD_BLOCK` zurück. Kritische Kontrollpushes werden journalisiert und nach Reconnect wiederholt. Telemetrie darf zusammengefasst werden.
- P hat keine direkte Antwort. Semantische READY-/APPLIED-/ACK-Pushes sind eigene Nachrichten mit RoundID/Sequenz als Kausalbezug; sie sind nötig, obwohl kein synchroner Request/Response-Call verwendet wird.

**Status und Folgeheader:** 200 vollständige Daten/Erfolg, 201 angelegt, 202 Adminoperation angenommen, 206 Bereich, 301 committed anderer Owner inkl. MapPatch, 302 temporäres gültiges Ziel, 400 ungültig, 401 Auth, 403 ACL, 404 fehlendes Objekt, 409 Kontrollkonflikt, 410 verlorene Range, 412 CAS/Precondition, 416 Bereich, 499 abgebrochener Stream, 501 spätere/nicht unterstützte Funktion, 503 derzeit nicht ausführbar, 507 physischer Platz/Quota fehlt. Numerische Werte als V, weil sie nicht in u8 passen. Tags: `1=ObjectInfo`, `2=MapPatch`, `3=Text Fehler`, `4=u64 RetryAfterMS` (Hinweis, kein Failure-Timeout), `5=ID OperationID`, `6=Vec(NodeAddress)`, `7=RangeResult`, `8=ID SubscriptionID`. Unbekannte optionale TLVs werden übersprungen. Erfolgsantworten können Map-Patches als Optimierung tragen. Bei HDD/SSD ist runtime_id Null, bei RAM aus §8.1.1 abgeleitet. Policy.storage_strategy kann CACHE nur mit späterer Capability enthalten. RAM unterstützt SINGLE und REPLICATION wie HDD/SSD; ERASURE bleibt für alle Strategien ein späteres Feature.

### 13.3 Gemeinsame binäre Strukturen

Notation `a:T,b:U` bedeutet Verkettung in genau dieser Reihenfolge. Die unten referenzierten Strukturen sind vollständig rekursiv definiert; Restbytes in einem v1-Body sind Fehler. Eine Schemaerweiterung benötigt Versions-/Capability-Aushandlung. Go-In-Memory-Layouts sind kein Wireformat.

```text
RoundID = membership:u64,generation:u64,initiator:ID,attempt:u64
Address = node:ID,url:Text
ObjectRef = bucket:ID,object:OID,map_revision:u64,owner_epoch:u64,runtime_id:ID
ObjectInfo = version:u64,length:u64,payload_digest:Opt(Digest),flags:u32,runtime_id:ID,
             compression:u8,layout_version:u16
Precondition = expected_version:Opt(u64),expected_length:Opt(u64),absent:Bool
RangeResult = offset:u64,length:u64,total_length:u64
Transfer = mode:u8,total_length:u64,digest:Digest,inline:Blob
  # mode 0 INLINE: inline enthält alle Bytes; mode 1 STREAM: inline leer,
  # CHUNK/TRANSFER_END folgen mit operation_id = ID der Ursprungsmessage
NodeAddress = node:ID,addresses:Vec(Text)
Location = node:ID,volume:ID,role:u8,actual_class:u8,fragment_index:u16,flags:u32
RangeEntry = start:OID,prefix_bits:u16,policy_version:u64,owner_epoch:u64,
             state:u8,runtime_id:ID,locations:Vec(Location)
MapPatch = membership:u64,generation:u64,revision:u64,bucket:ID,
           begin:OID,to_end:Bool,end_exclusive:OID,entries:Vec(RangeEntry),
           addresses:Vec(NodeAddress),policies:Vec(Policy)
  # end_exclusive nur relevant wenn !to_end; entries partitionieren [begin,end)
MapImage = revision:u64,buckets:Vec(BucketMap)
BucketMap = bucket:ID,entries:Vec(RangeEntry)
Policy = bucket:ID,version:u64,name:Text,state:u8,storage_strategy:u8,compression:u8,placement_mode:u8,
         redundancy:u8,replicas:u16,ec_k:u16,ec_m:u16,cache_enabled:Bool,
         cache_copies:u16,cache_budget:u64,accounting:u8,guaranteed:u64,
         unlimited:Bool,absolute:u64,metadata_reserve_ppm:u32
Grant = bucket:ID,user:ID,permissions:u32
User = id:ID,name:Text,role:u8,disabled:Bool
Member = id:ID,addresses:Vec(Text),state:u8,tags:Vec(Tag)
Tag = name:Text,value:Text
Membership = epoch:u64,cluster:ID,members:Vec(Member),excluded:Vec(ID)
StatsFilter = scope:u8,nodes:Vec(ID),volumes:Vec(ID),buckets:Vec(ID),include_ranges:Bool
StatsPart = source:ID,boot:ID,snapshot_seq:u64,part_index:u32,part_count:u32,
            digest:Digest,data:Blob
Decision = membership:u64,sequence:u64,previous:Digest,digest:Digest
Proof = node:ID,record_sequence:u64,digest:Digest
Certificate = Vec(Proof)
Reservation = node:ID,volume:ID,bucket:ID,physical_bytes:u64,purpose:u8
LogicalCredit = bucket:ID,primary:ID,credit_epoch:u64,available_growth:u64
Plan = round:RoundID,base_revision:u64,digest:Digest,next:MapImage,
       policies:Vec(Policy),membership:Opt(Membership),reservations:Vec(Reservation),
       logical_credits:Vec(LogicalCredit)
RangeRef = bucket:ID,start:OID,prefix_bits:u16
Cutover = round:RoundID,range:RangeRef,old_epoch:u64,new_epoch:u64,
          source:ID,target:ID,last_sequence:u64,dedupe_digest:Digest,
          subscription_digest:Digest,plan_digest:Digest
ControlChange = decision:Decision,kind:u8,payload:Blob
  # kind 0 ADMIN: payload=AdminChange; kind 1 LOGICAL_CREDITS: payload=CreditChange
AdminChange = admin_opcode:u16,admin_body:Blob
  # admin_body folgt dem Schema des angegebenen C-Admin-Commands;
  # bei USER_CREATE/USER_SET_PASSWORD enthält das Textfeld einen PHC-Verifier,
  # bei TOKEN_ROTATE enthält Blob den Cluster-Token für die Klartext-Node-Configs
CreditChange = bucket:ID,credit_epoch:u64,assignments:Vec(LogicalCredit),
               source_release_proofs:Certificate
Mutation = operation_id:ID,object:ObjectRef,kind:u8,offset:u64,
           new_length:u64,precondition:Precondition,cas_scope:u8,compare:Blob,payload:Transfer
  # kind 1 PUT,2 PUT_RANGE,3 APPEND,4 TRUNC,5 DELETE,6 CAS;
  # compare bei Nicht-CAS leer, cas_scope=0; payload ist Ersatz-/Appendinhalt
AdminAck = operation_id:ID,state:u8,entity:Opt(ID)
SubscriptionDescriptor = subscription:ID,client_session:ID,object:ObjectRef,
                         ranged:Bool,offset:u64,length:u64,cursor:u64,
                         version:u64,principal:ID,control_revision:u64
```

Enums: NodeState `0 ACTIVE,1 DRAINING,2 PAUSED,3 STOPPING,4 JOINING,5 EXCLUDED`; VolumeState `0 PENDING,1 ACTIVE,2 DRAINING,3 DETACHED,4 FAILED`; Class `0 HDD,1 SSD,2 RAM,3 RAMCACHE`; StorageStrategy `0 HDD,1 SSD,2 RAM,3 CACHE`; Role `0 PRIMARY,1 REPLICA,2 READ_CACHE,3 EC_DATA,4 EC_PARITY`; RangeState `0 STABLE,1 PREPARED,2 COPYING,3 CATCHING_UP,4 FENCED,5 LOST`; Compression `0 RAW,1 GZIP,2 LZ4,3 XZ`; PlacementMode `0 COUNT,1 EVERYWHERE`; Accounting `0 LOGICAL,1 PHYSICAL`; Redundancy `0 SINGLE,1 REPLICATION,2 ERASURE`; UserRole `0 USER,1 ADMIN,2 NODE`; Health `0 OK,1 WARN,2 ERROR`; Scope `0 LOCAL,1 CLUSTER`; AdminAck-State `0 REQUESTED,1 RUNNING,2 FULFILLED,3 FAILED`; ReasonBits `bit0 JOIN,1 DRAIN,2 VOLUME_ADD,3 VOLUME_REMOVE,4 LOAD,5 CAPACITY,6 POLICY,7 FORCE_DISCONNECT,8 ADMIN_TRIGGER`; Purpose `0 GUARANTEE,1 INFLIGHT,2 MIGRATION,3 OPERATING`; Bucket-State `0 ACTIVE,1 LOCKED,2 DELETING`; RebuildPhase `0 IDLE,1 ELECTING,2 COLLECTING,3 SEALED,4 PREPARED,5 MIGRATING,6 FINALIZING`; shutdown `0 DRAIN,1 SHORT,2 CLUSTER`. Permissions Bits ab 0: READ, WRITE, APPEND, DELETE, ADMIN_READ. Keine native LIST-Berechtigung. `Grant` kann nur Admin vergeben; eine Rolle ergibt sich niemals aus einem vom Client behaupteten Enum.

Statistik-Layouts sind exakt §7.1; insbesondere die verschachtelten IOStats-Felder und festen Arrays sind Teil des binären Schemas. `StatsPart.data` ist eine Partition der vollständig serialisierten StatsSnapshot-Bytefolge, kein JSON. Location.volume darf Null sein, wenn die Node das konkrete Volume innerhalb ihrer reservierten Kontingente lokal wählt; tatsächliche Volume-Zuordnung erscheint im StatsSnapshot. Bei expliziter Reservierung/Pinning muss die ID übereinstimmen.

CreditChange wird mit den vorhandenen CONTROL_PREPARE/READY/COMMIT/APPLIED-Nachrichten übermittelt. Alte Kontingentinhaber stoppen neue Reservationsvergaben für die betroffenen Credits, schließen bereits reservierte Mutationen ab und belegen ihren freigegebenen Rest vor der Umverteilung. Erst die neue credit_epoch aktiviert die assignments; alte Credits dürfen danach nicht weiter verbraucht werden. Die Kontrollentscheidung prüft verbleibende Bucket-Nutzung plus alle neuen logischen Wachstumskontingente gegen A; ein geänderter Plan benutzt denselben Übergabevertrag. Dadurch kann kein Kontingent zeitgleich bei altem und neuem Primary gültig sein.

ObjectInfo.payload_digest darf fehlen, wenn kein vollständiger Digest ohne Gesamtobjekt-Scan verfügbar ist; Transfer-/Recordchecksummen bleiben zwingend. Ein 10-Byte-Append darf für einen vollständigen Objektdigest keinen O(n)-Scan auslösen.

Round-/Snapshot-Digests sind SHA-256 über kanonische Bytes, ohne ihr eigenes Digest-Feld; bei Plan/Decision werden die übrigen Felder in Tabellenreihenfolge gehasht. Certificates sind nach Node-ID sortiert und enthalten genau einen Proof jedes erforderlichen Mitglieds. Das Modell schützt vor Crash/Netzfehlern, nicht vor bösartigen authentifizierten Cluster-Nodes; Proofs sind validierte Journalbelege aus gegenseitig authentifizierten Verbindungen, keine erfundene byzantinische Signaturgarantie.

### 13.4 Gemeinsame Session-/Streaming-/Statistiknachrichten

Alle `=>`-Layouts beschreiben `result` einer erfolgreichen RESPONSE. Ohne `=>` und bei Q ist `result` leer; Status/Folgeheader tragen den Abschluss. Fehler-RESPONSEs haben leeres result. F/P haben nie ein RESPONSE-Layout.

| Code | Name / Senderrolle / Modus | Body und erfolgreiche Antwort |
|---|---|---|
| 0x0001 | HELLO / alle / Q | `min_version:u16,max_version:u16,capabilities:u64,client_name:Text` => `version:u16,capabilities:u64,node:ID,cluster:ID,membership:u64,map_revision:u64` |
| 0x0002 | PING / alle / Q/F | `nonce:u64` => `nonce:u64` |
| 0x0003 | GOAWAY / Server / P | `reason:u8,operation:ID,addresses:Vec(NodeAddress),membership:u64,map_revision:u64` |
| 0x0004 | EVENT / Server / P | `cause:ID,status:V,message:Text` |
| 0x0005 | TRANSFER_CHUNK / Besitzer des Transfers / F/P | `operation:ID,part_index:u64,offset:u64,data:Blob` |
| 0x0006 | TRANSFER_END / Besitzer des Transfers / F/P | `operation:ID,total_length:u64,digest:Digest` |
| 0x0007 | CANCEL / alle / F | `operation:ID` |
| 0x0008 | STATS_GET / Admin,Node / Q | `filter:StatsFilter` => `snapshot:StatsSnapshot` oder gestreamt als StatsPart-Pushes, result dann `subscription:ID,snapshot_count:u32` |
| 0x0009 | STATS_SUBSCRIBE / Admin,Node / Q/F | `subscription:ID,filter:StatsFilter,interval_ms:u32` => `subscription:ID` |
| 0x000a | STATS_UNSUBSCRIBE / Admin,Node / Q/F | `subscription:ID` |
| 0x000b | STATS_SNAPSHOT / Server / P | `subscription:ID,part:StatsPart` |
| 0x000c | MAP_PATCH / Server / P | `patch:MapPatch` |
| 0x000d | TRANSFER_BEGIN / Server / P | `operation:ID,total_length:u64,digest:Digest,info:Opt(ObjectInfo),range:Opt(RangeResult)` |

REBUILD_CONFLICT/REBUILD_ABORT sind reservierte vorläufige Nachrichten. Ihre Notwendigkeit, Reason-Codes und genaue Race-Semantik werden erst nach §4.2-Stresstests entschieden; keine feste Grant-/ABORT-Implementierung aus diesem Katalog ableiten. Nach verbindlicher Koordinatorbindung wird ein abgelehnter Plan vom selben Koordinator korrigiert; COPY/Progress-Kennungen binden immer denselben Range-/Round-Stand.

GOAWAY-Reason `0 DRAINED,1 SHORT_SHUTDOWN,2 CLUSTER_SHUTDOWN,3 EXCLUDED,4 PROTOCOL_ERROR`. HELLO ist Versionsaushandlung nach HTTP-Auth, kein zusätzlicher Login. HTTP `/api/message` unterstützt Q mit INLINE-Bodies; umfangreiches Streaming und Push erfordern WebSocket. STATS_GET-Antwortvariante wird mit Capability und Folgeheader-Tag `9=u8 encoding` (`0 INLINE,1 PARTS`) eindeutig markiert; PARTS werden unter ID des Requests vor der terminalen RESPONSE gesendet. Ein Snapshot-Count umfasst die angefragten erreichbaren Nodes, fehlende Nodes werden als Stale-Kontaktstatus im EVENT gemeldet.

### 13.5 Client-Objektnachrichten

Normale Clients bekommen ausschließlich Objektzugriff und die dafür nötigen Routing-/Bucketinformationen. Kein Cluster-Stats, Volume-/Userzugriff oder serverseitiger Dateisystem-/S3-Namespace.

| Code | Name / Modus | Body und erfolgreiche Antwort |
|---|---|---|
| 0x0100 | GET / Q | `object:ObjectRef` => `info:ObjectInfo,data:Transfer` |
| 0x0101 | GET_RANGE / Q | `object:ObjectRef,offset:u64,length:u64` => `info:ObjectInfo,range:RangeResult,data:Transfer` |
| 0x0102 | PUT / Q/F | `object:ObjectRef,pre:Precondition,data:Transfer` => `info:ObjectInfo` |
| 0x0103 | PUT_RANGE / Q/F | `object:ObjectRef,offset:u64,pre:Precondition,data:Transfer` => `info:ObjectInfo,range:RangeResult` |
| 0x0104 | APPEND / Q/F | `object:ObjectRef,pre:Precondition,data:Transfer` => `info:ObjectInfo,start_offset:u64` |
| 0x0105 | TRUNC / Q/F | `object:ObjectRef,length:u64,pre:Precondition` => `info:ObjectInfo` |
| 0x0106 | DELETE / Q/F | `object:ObjectRef,pre:Precondition` |
| 0x0107 | STAT / Q | `object:ObjectRef` => `info:ObjectInfo` |
| 0x0108 | CAS / Q/F | `object:ObjectRef,scope:u8,offset:u64,pre:Precondition,expected:Blob,replacement:Blob` => `info:ObjectInfo` |
| 0x0109 | OP_STATUS / Q | `operation:ID,object:ObjectRef` => `state:u8,original_opcode:u16,original_response:Blob` |
| 0x010a | SYNC_BARRIER / Q | `object:ObjectRef,through_version:u64` => `durable_version:u64` |
| 0x010b | MAP_GET / Q | `bucket:ID,hash:OID` => `patch:MapPatch` |
| 0x010c | BUCKET_INFO / Q | `name:Text` => `policy:Policy,permissions:u32` |
| 0x0110 | SUBSCRIBE / Q, später | `object:ObjectRef,subscription:ID,ranged:Bool,offset:u64,length:u64,resume:Opt(u64)` => `subscription:ID,cursor:u64,info:ObjectInfo,data:Transfer` |
| 0x0111 | UNSUBSCRIBE / Q/F, später | `subscription:ID` |
| 0x0112 | OBJECT_UPDATE / P vom Server, später | `subscription:ID,cursor:u64,previous_version:u64,new_version:u64,part:u32,parts:u32,change:u8,offset:u64,new_length:u64,data:Transfer` |
| 0x0113 | SUBSCRIPTION_MOVED / P vom Server, später | `subscription:ID,cursor:u64,patch:MapPatch` |
| 0x0114 | SUBSCRIPTION_ACK / F, später | `subscription:ID,through_cursor:u64` |
| 0x0115 | BUCKET_MAINTENANCE / Q, Single-Node | `object:ObjectRef` (OID=0, nur Bucket relevant) => leer; exklusive Wartung an die Bucket-WebSocket-Sitzung binden |
| 0x0116 | BUCKET_SCAN / Q, Single-Node | `object:ObjectRef` (OID=0), `after:Opt(OID),limit:u32` (1..1000) => `count:u32,(id:OID,info:ObjectInfo)*,next:Opt(OID)`; nur in eigener Wartungssitzung |

CAS-Scope `0 RANGE,1 OBJECT`; OP_STATUS-State `0 UNKNOWN,1 PENDING,2 COMMITTED,3 FAILED,4 EXPIRED`. UNKNOWN/EXPIRED ist keine Zusicherung „nie ausgeführt“. ObjectUpdate-Change `0 REPLACE,1 RANGE_WRITE,2 APPEND,3 TRUNC,4 DELETE,5 RESYNC_REQUIRED`. SUBSCRIBE/UNSUBSCRIBE/ACK sind in v1 reserviert: Q => 501; F-Varianten werden mangels ausgehandeltem Feature nicht gesendet.

**Streaming-Regel:** PUT/PUT_RANGE/APPEND mit mode=STREAM eröffnen unter ihrer ID einen begrenzten Staging-Transfer. CHUNKs tragen explizite Offsets; END validiert Gesamtlänge/Digest. Erst dann darf eine Mutation sichtbar werden und Q seinen terminalen Erfolg bekommen. Bei Abbruch werden uncommittete Extents freigegeben. Für GET/SUBSCRIBE-Snapshots kann der Server CHUNK/END-Pushes unter der Request-ID senden und im terminalen result Transfer(mode=STREAM) referenzieren; Clientpufferung ist begrenzt/spoolbar. Nicht vorbereitete CHUNKs werden abgewiesen; der Server kündigt Downstream-Streams vor dem ersten Chunk durch TRANSFER_BEGIN an. Das ist keine zweite RESPONSE und kein Commit-ACK. Bei späteren OBJECT_UPDATE-Streams ist operation die ID des zugehörigen Pushes; dessen Cursor-/Versionsmetadaten werden vor den Chunks gesendet. Kleinere Requests anderer IDs dürfen dazwischenliegen. Flow Control erfolgt über Queue-/Staging-Budgets und WebSocket/TCP-Backpressure.

### 13.6 Node-/Wahl-/Plan-/Replikationsnachrichten

Nur authentifizierte NODE-Rolle, gebunden an Membership-Epoch. In der Tabelle bedeutet `P` dauerhafte/periodische Zustandsübermittlung ohne korrelierte RESPONSE. Auf Q/F-Varianten können menschliche Diagnosetools warten, normale Peerabläufe nutzen überwiegend Pushes.

| Code | Name / Modus | Body und erfolgreiche Antwort |
|---|---|---|
| 0x1000 | NODE_HELLO / Q | `cluster:ID,node:ID,membership:u64,addresses:Vec(Text),boot:ID` => `membership:Membership,control_sequence:u64,generation:u64,map_revision:u64` |
| 0x1001 | JOIN / Q | `member:Member,cluster:ID` => `membership:Membership` (erst nach committed Aufnahme) |
| 0x1002 | RESUME_STATE / P | `node:ID,control:ControlWire,proofs:Vec(Proof)` |
| 0x1003 | REBUILD_NEEDED / F | `membership:u64,generation:u64,reasons:u64,state_sequence:u64` |
| 0x1004 | REBUILD_OFFER / P | `round:RoundID,candidate:ID,reasons:u64` |
| 0x1005 | REBUILD_CONFLICT / P | `round:RoundID,holder:RoundID,preferred:ID` |
| 0x1006 | REBUILD_ABORT / P | `round:RoundID,reason:u8,preferred:ID` |
| 0x1007 | STATS_FRAGMENT / P | `round:RoundID,part:StatsPart,control_sequence:u64,pending_changes:Vec(ID)` |
| 0x1008 | REBUILD_SEALED / P | `round:RoundID,certificate:Certificate` |
| 0x1009 | PLAN_PREPARE / P | `plan:Plan,decision:Decision` (groß via ControlTransfer) |
| 0x100a | PLAN_READY / P | `round:RoundID,decision:Decision,status:V,proof:Proof` |
| 0x100b | PLAN_COMMIT / P | `round:RoundID,decision:Decision,certificate:Certificate` |
| 0x100c | PLAN_APPLIED / P | `round:RoundID,decision:Decision,proof:Proof` |
| 0x100d | COPY_BEGIN / Q/F | `round:RoundID,range:RangeRef,source:ID,target:ID,snapshot_seq:u64,transfer:ID` |
| 0x100e | COPY_RECORD / P | `transfer:ID,record_sequence:u64,record:Blob` |
| 0x100f | COPY_END / P | `transfer:ID,last_sequence:u64,records_digest:Digest` |
| 0x1010 | APPLY_LOG / P | `round:RoundID,range:RangeRef,first_sequence:u64,records:Vec(Blob)` |
| 0x1011 | RANGE_PROGRESS / P | `round:RoundID,range:RangeRef,copied_physical:u64,durable_sequence:u64,remaining_physical:u64,handoff_pending:u64` |
| 0x1012 | SOURCE_FENCE / Q/F | `cutover:Cutover` => `last_sequence:u64,fence_proof:Proof` |
| 0x1013 | CUTOVER_PREPARE / P | `cutover:Cutover,source_proof:Proof,target_proof:Proof,decision:Decision` |
| 0x1014 | CUTOVER_READY / P | `cutover:Cutover,decision:Decision,status:V,proof:Proof` |
| 0x1015 | CUTOVER_COMMIT / P | `cutover:Cutover,decision:Decision,certificate:Certificate` |
| 0x1016 | CUTOVER_APPLIED / P | `cutover:Cutover,decision:Decision,proof:Proof` |
| 0x1017 | CLEANUP / Q/F | `round:RoundID,range:RangeRef,through_epoch:u64` |
| 0x1018 | FINALIZE_PREPARE / P | `round:RoundID,decision:Decision,needrebuild:Bool` |
| 0x1019 | FINALIZE_READY / P | `round:RoundID,decision:Decision,proof:Proof` |
| 0x101a | FINALIZE_COMMIT / P | `round:RoundID,decision:Decision,certificate:Certificate` |
| 0x101b | FINALIZE_APPLIED / P | `round:RoundID,decision:Decision,proof:Proof` |
| 0x101c | REPLICA_PREPARE / P | `membership:u64,range:RangeRef,owner_epoch:u64,sequence:u64,mutation:Mutation` |
| 0x101d | REPLICA_READY / P | `operation:ID,sequence:u64,status:V,proof:Proof` |
| 0x101e | REPLICA_COMMIT / P | `operation:ID,sequence:u64,certificate:Certificate` |
| 0x101f | REPLICA_COMMITTED / P | `operation:ID,sequence:u64,digest:Digest` |
| 0x1020 | REPLICA_ABORT / P | `operation:ID,sequence:u64` |
| 0x1021 | CONTROL_PREPARE / P | `change:ControlChange` |
| 0x1022 | CONTROL_READY / P | `decision:Decision,status:V,proof:Proof` |
| 0x1023 | CONTROL_COMMIT / P | `decision:Decision,certificate:Certificate` |
| 0x1024 | CONTROL_APPLIED / P | `decision:Decision,proof:Proof` |
| 0x1025 | NODE_STATE / P | `node:ID,state:u8,state_sequence:u64,operation:ID` |
| 0x1026 | SHUTDOWN_PREPARE / P | `shutdown:ID,mode:u8,membership:u64` |
| 0x1027 | SHUTDOWN_READY / P | `shutdown:ID,node:ID,control_sequence:u64,proof:Proof` |
| 0x1028 | SHUTDOWN_STOP / P | `shutdown:ID,certificate:Certificate` |
| 0x1029 | SUBSCRIPTIONS_HANDOFF / P, später | `round:RoundID,range:RangeRef,last_object_sequence:u64,descriptors:Vec(SubscriptionDescriptor),events:Vec(Blob),digest:Digest` |
| 0x102a | SUBSCRIPTIONS_READY / P, später | `round:RoundID,range:RangeRef,digest:Digest,proof:Proof` |
| 0x102b | CONTROL_TRANSFER_BEGIN / F | `operation:ID,opcode:u16,length:u64,digest:Digest` |
| 0x102c | CONTROL_TRANSFER_END / F | `operation:ID,digest:Digest` |
| 0x102d | MEMBERSHIP_PREPARE / P | `decision:Decision,next:Membership,forceful:Bool,accept_data_loss:Bool,external_fencing_confirmed:Bool` |
| 0x102e | MEMBERSHIP_READY / P | `decision:Decision,status:V,proof:Proof` |
| 0x102f | MEMBERSHIP_COMMIT / P | `decision:Decision,certificate:Certificate` |
| 0x1030 | MEMBERSHIP_APPLIED / P | `decision:Decision,proof:Proof` |
| 0x1031 | RAM_RESTORE / Q/F | `range:RangeRef,runtime_id:ID,target:ID,transfer:ID` => `runtime_id:ID,last_sequence:u64` |
| 0x1032 | RUNTIME_RESET_PREPARE / P | `decision:Decision,range:RangeRef,previous_runtime:ID,new_runtime:ID,empty_proofs:Certificate` |
| 0x1033 | RUNTIME_RESET_READY / P | `decision:Decision,proof:Proof` |
| 0x1034 | RUNTIME_RESET_COMMIT / P | `decision:Decision,certificate:Certificate` |
| 0x1035 | RUNTIME_RESET_APPLIED / P | `decision:Decision,proof:Proof` |
| 0x1036 | PEER_ADMIN_REQUEST / Q | `actor:ID,username:Text,operation:ID,opcode:u16,body:Blob` => vollständiger innerer RESPONSE-Body (`status:V,follow_headers:Blob,result`); nur nach NODE_HELLO, Ziel prüft den aktuellen Admin-Account selbst |
| 0x1037 | PEER_VERIFY_ADMIN / Q | `actor:ID,username:Text` => `actor:ID,username:Text`; Gegenrichtung derselben gebundenen Peer-Verbindung, Abgleich mit aktuellem autoritativem Accountstand, keine Rollenbehauptung und kein Membership-/Commit-Beleg |
| 0x1038 | PEER_DIRECTORY / F, Q für Diagnose | `cluster:ID,sender:ID,nodes:Vec(NodeAddress)`; nur geprüfte Peer-Identität, Absender-ID muss dem gebundenen Peer entsprechen; persistiert/vereinigt Kontaktinformationen und verteilt Änderungen weiter, keine Membership- oder Placement-Autorität |
| 0x1040 | GLOBAL_RESERVE / Q | `claim:GlobalClaim` => leer; dauerhafte Reservierung des globalen Membership-/Admin-Slots |
| 0x1041 | GLOBAL_PREPARE / Q | `proposal:GlobalProposal` => `proof:Proof`; vollständiger, gehashter und geflushter Systemobjektgraph ist Voraussetzung für READY |
| 0x1042 | GLOBAL_COMMIT / Q | `schema:u16=1,decision:Decision,ready:Blob(Certificate)` => `proof:Proof`; exakt ein READY jedes gebundenen Teilnehmers |
| 0x1043 | GLOBAL_APPLY / Q | `schema:u16=1,decision:Decision` => `proof:Proof`; erst nach dauerhaftem COMMIT, Veröffentlichung des System-Heads und Installation der Laufzeitsicht |
| 0x1044 | GLOBAL_RELEASE / Q | `schema:u16=1,decision:Decision,applied:Blob(Certificate)` => `proof:Proof`; genau ALL-APPLIED, wiederholbar auch nach Freigabe |
| 0x1045 | GLOBAL_ABORT / Q | `claim:GlobalClaim` => `proof:Proof`; persistierte Absage einer nicht committed Runde, auch ohne vorherige Reservierung |

Diese Q-Nachrichten bilden den explizit bestätigten Teilnehmerpfad für **globale** Membership-/Admin-Entscheidungen. Sie ersetzen keinen Range-Scope und legen dessen späteren Race-/Wahlalgorithmus nicht fest. Sie sind nur nach NODE_HELLO zulässig; der gebundene Absender muss der Initiator der referenzierten Runde sein. Ein noch nicht aktivierter dauerhafter Controller antwortet 503. Unvollständiger oder widersprüchlicher Zustand erzeugt keine Erfolgsantwort. P-Nachrichten des Katalogs bleiben die gesonderten Push-Varianten; Q und P werden nicht stillschweigend als identische Feldfolgen interpretiert.

```text
GlobalBaseMembership = membership:Membership,control_sequence:u64=0,
                       generation:u64=1,map_revision:u64=0
GlobalClaim = schema:u16=1,membership:Blob(GlobalBaseMembership),
              sequence:u64,head:Digest,generation:u64,map_revision:u64,
              system_image:Digest,round:RoundID,required:Vec(ID)
GlobalProposal = schema:u16=1,claim:Blob(GlobalClaim),kind:u16,
                 system_image:Digest,has_next_membership:Bool,
                 [next_membership:Blob(GlobalBaseMembership)]
```

`kind=1` ändert Membership, `kind=2` globale Administration. `required` ist streng nach Node-ID sortiert, duplikatfrei, höchstens 10.000 Einträge. Bei Membership-Änderung ist die Vereinigung aus bisherigen und neuen Mitgliedern erforderlich; bei Administration die unveränderte vollständige Membership. `next_membership` ist genau bei `kind=1` vorhanden. Die eingebettete Membership verwendet den vorhandenen NODE_HELLO-Antwortcodec mit den oben festen drei Zusatzwerten; die tatsächlichen Ausgangszähler stehen separat im Claim. Ein System-Image ist ein normales SHA-256-adressiertes Manifestobjekt des EVERYWHERE-System-Buckets mit Referenzen auf Account-/PG-/Bucketobjekte und Map-Checkpoints. Die Veröffentlichung seines Heads erfolgt atomar erst nach ALL-COMMIT; reine Übertragung seiner Kinder ändert noch keine Authentifizierung oder Placement-Autorität. Die Aktivierung im Daemon benötigt zusätzlich Bootstrap, Wiederaufnahme und Installation dieser Sicht.

Für diesen globalen Teilnehmerpfad wird `Decision.digest` als SHA-256 über `Text("STQR control decision v1"), membership:u64, sequence:u64, previous:Digest, kind:u16, payload:Blob` berechnet. `payload` ist die kanonische Folge `Blob(GlobalBaseMembership), RoundID, generation:u64, map_revision:u64, system_image:Digest, required:Vec(ID), proposed_system_image:Digest, has_next_membership:Bool, [Blob(GlobalBaseMembership)]`. Damit sind Ausgangs-Membership, Initiator/Versuch, Ausgangs-Checkpoint, Teilnehmermenge, Objektgraph und Ziel-Membership unabhängig von der zustellenden Verbindung gebunden. Leere Go-Slices und nil-Slices sind auf dem Wire dieselbe leere Vec und dürfen nach Reconnect keine unterschiedlichen Zusagen ergeben.

```text
ControlWire = membership:u64,generation:u64,round:RoundID,coordinator:ID,
              phase:u8,needrebuild:Bool,snapshot_digest:Digest,
              plan_digest:Digest,decision_sequence:u64
```

RAM_RESTORE nutzt COPY_RECORD/APPLY_LOG unter derselben transfer-ID für Daten/Versionen/Dedupe und endet mit COPY_END; beim Q-Abschluss ist die wiederhergestellte Kopie vollständig validiert, aber eine neue Ownership darf nur nach den allgemeinen Regeln aktiviert werden. Ein Runtime-Reset benötigt Verlust-/Leerbelege aller bisherigen erforderlichen RAM-Replikate bzw. die ausdrücklich freigegebene Ausschlussentscheidung; neue Map-Revision und Runtime-ID werden vor Wiederaufnahme gemeinsam veröffentlicht.

ControlTransfer transportiert einen großen bereits definierten Body unter reservierter ID via TRANSFER_CHUNK; END validiert Länge/Digest und dispatcht den ursprünglichen Opcode genau einmal. Zulässig nur für NODE-Pushes. COPY_RECORD/APPLY_LOG transportieren **logische** Records gemäß folgendem kanonischen Schema, unabhängig von physischen Disk-Offsets oder RAM-Zeigern:

```text
CopyRecord = schema:u16=1,kind:u8,range_sequence:u64,object:OID,
             version:u64,logical_length:u64,logical_offset:u64,extent_length:u64,
             last_part:Bool,payload:Blob,dedupe:Opt(DedupeRecord)
DedupeRecord = operation:ID,request_digest:Digest,opcode:u16,status:V,
               result:Blob,object_version:u64,append_offset:u64
```

Kind `0 OBJECT_EXTENT,1 TOMBSTONE,2 DEDUPE_ONLY,3 ARENA_DESCRIPTOR,4 ZERO_EXTENT`. ZERO_EXTENT enthält nur logischen Offset und extent_length, Payload ist leer; damit wird bei Migration kein riesiger Nullbereich übertragen. Bei OBJECT_EXTENT ist extent_length gleich der unkomprimierten Payloadlänge. OBJECT_EXTENT-Payload besteht aus unkomprimierten logischen Bytes und wird am Ziel gemäß Zielpolicy komprimiert; physische Codec-Bytes werden nicht als Anwendungsdaten missverstanden. Extent-Payload umfasst maximal 1 MiB und ist an logical_offset einzufügen; last_part beendet die Snapshot-Version dieses Objekts. TOMBSTONE hat leeren Payload; DEDUPE_ONLY enthält einen DedupeRecord ohne Payload. ARENA_DESCRIPTOR benutzt logical_offset für den Tail-Anfang und Payload=`capacity:u64,used:u64`; die Zielnode allokiert eigenen physischen Platz. Result im DedupeRecord ist das originale erfolgreiche/fehlgeschlagene opcode-spezifische RESPONSE-result aus §13, kein roher In-Memory-Puffer. COPY_END prüft den Digest der geordneten Recordfolge und den letzten vollständigen Range-Stand; Teilobjekte werden nicht publiziert. APPLY_LOG nutzt dieselbe Form für neue Extents/Tombstones/Dedupe, geordnet nach range_sequence; ein neuer logischer Längenstand ersetzt die vorige Extent-Sicht erst beim letzten Teil derselben Version. Der Transport benötigt dadurch keinen fertig festgelegten physischen On-Disk-Recordcodec. Für späteres Subscription-Replay sind `events` kanonisch kodierte OBJECT_UPDATE-Bodies. Fehlende Unterstützung wird vor dem Plan als Capability-Konflikt erkannt.

### 13.7 Vollständige Administration

ADMIN oder ausdrücklich ADMIN_READ für reine Leseoperationen. `C` bedeutet globale CONTROL_CHANGE; `L` lokal im angesprochenen Daemon persistent; `G` löst einen Generationsgrund aus. Annahme einer lang laufenden Operation antwortet 202 + AdminAck und später EVENT/Stats-Status; ein 202 ist kein abgeschlossener Drain/Policywechsel. Q liefert immer auch Ablehnungen wie 403/507; F hat keinen Rückkanalvertrag.

| Code | Name / Modus / Wirkung | Body und erfolgreiche Antwort |
|---|---|---|
| 0x2000 | ADMIN_GET / Q | `section:u8` => `image:AdminImage` |
| 0x2001 | VOLUME_ADD / Q/F / L,G | `name:Text,source:u8,path:Text,class:u8,size_bytes:u64` => `ack:AdminAck` |
| 0x2002 | VOLUME_REMOVE / Q/F / L,G | `volume:ID` => `ack:AdminAck` |
| 0x2003 | BUCKET_CREATE / Q/F / C,G | `policy:Policy` => `ack:AdminAck` |
| 0x2004 | BUCKET_SET_POLICY / Q/F / C,G | `policy:Policy,expected_policy_version:u64` => `ack:AdminAck` |
| 0x2005 | BUCKET_SET_QUOTA / Q/F / C | `bucket:ID,accounting:u8,guaranteed:u64,unlimited:Bool,absolute:u64,metadata_reserve_ppm:u32,expected_policy_version:u64` => `ack:AdminAck` |
| 0x2006 | BUCKET_RENAME / Q/F / C | `bucket:ID,name:Text` => `ack:AdminAck` |
| 0x2007 | BUCKET_LOCK / Q/F / C | `bucket:ID,locked:Bool` => `ack:AdminAck` |
| 0x2008 | BUCKET_DELETE / Q/F / C,G | `bucket:ID` => `ack:AdminAck` |
| 0x2009 | BUCKET_GRANT / Q/F / C | `grant:Grant` => `ack:AdminAck` |
| 0x200a | BUCKET_REVOKE / Q/F / C | `bucket:ID,user:ID` => `ack:AdminAck` |
| 0x200b | USER_CREATE / Q/F / C | `user:User,password:Text` => `ack:AdminAck` |
| 0x200c | USER_UPDATE / Q/F / C | `user:User` => `ack:AdminAck` |
| 0x200d | USER_SET_PASSWORD / Q/F / C | `user:ID,password:Text` => `ack:AdminAck` |
| 0x200e | USER_DELETE / Q/F / C | `user:ID` => `ack:AdminAck` |
| 0x200f | CLUSTER_TOKEN_ROTATE / Q/F / C | `version:u64,new_token:Blob` => `ack:AdminAck` |
| 0x2010 | NODE_DRAIN / Q/F / L,G | `node:ID` => `ack:AdminAck` |
| 0x2011 | NODE_SHUTDOWN / Q/F / L | `node:ID,mode:u8` => `ack:AdminAck` |
| 0x2012 | CLUSTER_SHUTDOWN / Q/F / C | `mode:u8=2` => `ack:AdminAck` |
| 0x2013 | FORCE_DISCONNECT / Q/F / C,G | `nodes:Vec(ID),accept_data_loss:Bool,external_fencing_confirmed:Bool,expected_membership:u64` => `ack:AdminAck` |
| 0x2014 | NODE_SET_TAGS / Q/F / C,G | `node:ID,tags:Vec(Tag)` => `ack:AdminAck` |
| 0x2015 | PEER_CONNECT / Q/F / L | `url:Text` => `ack:AdminAck` |
| 0x2016 | REBUILD_TRIGGER / Q/F / G | `reasons:u64` => `ack:AdminAck` |
| 0x2017 | SCRUB_START / Q/F / L | `volume:ID` => `ack:AdminAck` |
| 0x2018 | AUDIT_GET / Q | `after_sequence:u64,limit:u32` => `entries:Vec(AuditEntry)` |
| 0x2019 | ADMIN_OPERATION_STATUS / Q | `operation:ID` => `ack:AdminAck,status:V,detail:Text` |
| 0x201a | NODE_CONFIG_GET / Q / L | leer => `config_json:Text` (versionierte lokale Feldansicht ohne Secrets, aktiver Listener/Volumes-Pfad und restart_required) |
| 0x201b | NODE_CONFIG_SET / Q / L | `field_patch_json:Text` (typisiertes JSON-Objekt einzelner erlaubter Felder) => `config_json:Text`; Validierung/atomare Config-Persistenz; Identitäten/verwaltete Volume-Liste nicht überschreibbar |
| 0x201c | VOLUME_CANDIDATES / Q / L | leer => `candidates_json:Text` (JSON der Library-Structs aus §8.2.2) |
| 0x201d | VOLUME_RESCAN / Q/F / L,G | `volume:ID` => `ack:AdminAck`; vorhandenes Backend neu auf Kapazität prüfen und zusätzlich nutzbaren Platz übernehmen, kein Resize des Devices selbst |
| 0x201f | VOLUME_GROW_INFO / Q / L | `volume:ID` => `kind:u8,current:u64,available:u64,maximum:u64`; kind 0 Datei, 1 Blockdevice, 2 RAM; Größen in Bytes; Datei/RAM maximal current+available (auf Seiten abgerundet, int64-Grenze), Blockdevice volle physische Größe |
| 0x2020 | VOLUME_GROW / Q / L,G | `volume:ID,size_bytes:u64` => `ack:AdminAck`; Datei/RAM explizite Zielgröße, Blockdevice size_bytes=0 übernimmt volle physische Größe; niemals schrumpfen, bei Platzmangel 507 |
| 0x2021 | USER_LIST / Q | leer => `users:Vec(User)`; nach Benutzername sortiert, ausschließlich öffentliche Accountfelder, keine Passwortverifier |
| 0x2022 | BUCKET_LIST / Q | `after_name:Text,limit:u32` (1..1000) => `page_json:Text` mit `{buckets:[BucketDescription...],next:Text}`; leeres after_name startet, leeres next beendet die Liste; nur Admin, User-Freigaben als Unterarray je Bucket, keine internen Retry-Belege; Listenansicht ergänzt bei endlicher logischer Quota `used_bytes:u64` als Live-Belegung (bei Lesefehlern stattdessen `usage_error:Text`, niemals eine vorgetäuschte Nullbelegung) |
| 0x2023 | BUCKET_QUOTA_CAPACITY / Q | `bucket:ID,strategy:u8,object_size_profile:u8,previous_overhead_ppm:u32` => `used:u64,available_physical:u64,reserved_other_physical:u64,maximum:u64,nodes:u32,overhead_ppm:u32,profiles:Vec(key:u8,label:Text,typical_bytes:u64,overhead_ppm:u32)`; ID null = neuer Bucket; Profil 255 bewahrt eine bestehende Pauschale, sonst wird sie aus dem Format berechnet |
| 0x201e | VOLUME_CAPACITY_GET / Q / L | leer => `filesystem_available:u64,ram_available:u64,ram_limit:u64`; Available-Wert für das aktive Volume-Verzeichnis, verfügbarer System-RAM und wirksame RAM-Grenze nach optionalem Budget |
| 0x2024 | NODE_ADMIN_REQUEST / Q/F / L | `target:ID,opcode:u16,body:Blob` => Status/Folgeheader/result des inneren Befehls; Operations-ID bleibt die ID des äußeren Requests; kein beliebiges URL-Ziel |

`NODE_ADMIN_REQUEST` erlaubt ausschließlich Volume-Anlegen/-Entfernen/-Rescan/
-Vergrößern, Volume-Kandidaten/Kapazität/Wachstumsgrenzen, lokale Config-Ansicht/
Feldänderungen und Operationsstatus. Rekursive Weiterleitung, Objekt-, Bucket-
und Benutzerbefehle sind ausgeschlossen. Nur die Ziel-Node speichert ihre
Volume-Konfiguration und lokalen Operationsbelege. Der interne Peer-Aufruf
unterscheidet Transporterfolg (äußere Antwort) und den vollständigen inneren
Admin-Response-Body; der Dashboard-Proxy reicht dessen Status unverändert durch,
insbesondere 201, 202 und Fehler. Das Ziel ermittelt die Rolle aus seinem
System-Bucket, niemals aus einer mitgesendeten Rollenbehauptung.

HELLO-Capabilities: Bit 0 (`1`) bezeichnet Peer-Identitätsbindung/Telemetrie;
Bit 1 (`2`) bezeichnet gezielte Node-Administration. Bit 2 (`4`) kennzeichnet einen noch nicht aufgenommenen Beitrittsknoten. Dieser liefert in HELLO und NODE_HELLO_ACK Membership-Epoch/Generation/Map-Revision 0 und eine leere Mitgliederliste; Cluster-ID und Transportidentität bleiben vorhanden. NODE_HELLO darf deshalb für die reine Vorbeitritts-Identitätsbindung membership=0 tragen. Das bestätigt keine Aufnahme oder Objekt-Autorität. Diese Bits behaupten weder
Membership-Aufnahme noch funktionierendes Rebuild/Placement. Die spätere
Membership-Steuerung muss die Peer-Verbindung vor Freigabe dieser Abläufe
an den committed Kontrollstand binden.

Der erste Single-Node-Dashboard-Stand verwendet bereits diese Nachrichten, VOLUME_ADD/REMOVE und STATS_SUBSCRIBE/SNAPSHOT/UNSUBSCRIBE über binäre `stqr.v1`-WebSockets. Quelle bei VOLUME_ADD: `0 FILE,1 BLOCKDEVICE,2 RAM`. Empfang, Dispatch und Senden haben getrennte begrenzte Queues; Antworten werden korreliert, Telemetrie darf nur als vollständiger neuer Snapshot einen älteren ersetzen. Der CLI-Einstieg sammelt `--config`, `--port`, `--listen`, `--connect`, `--name`, `--advertise`, `--data-dir`, `--volumes-dir`, `--ram-bytes`, `--ramcache-bytes`, `--system-volume-bytes` und `--allow-subnet`. Standardport 7419, Standardzugriff nur IPv4-/IPv6-Loopback, erweiterbar auf explizite CIDRs. Listener/Verzeichnisse/RAM-Budget benötigen nach Config-Änderung einen Neustart; Name/Advertise/Subnetze gelten unmittelbar. Bekannte authentifizierte Kontakte verbinden sich automatisch und tauschen Statistik-Pushes sowie das persistente Peer-Verzeichnis aus. Ein neuer vorbereiteter Beitrittsknoten bleibt als JOINING sichtbar und für normale Authentifizierung/Objektzugriffe gesperrt, bis Systemkopie und Membership committed aktiviert sind. Die automatische Kontaktaufnahme allein ist kein fertiger Storage-Join; fehlende Clusteridentität/-Credentials erzeugen keinen neuen Ersatzcluster.


```text
AdminImage = membership:Membership,control:ControlWire,policies:Vec(Policy),
             users:Vec(User),grants:Vec(Grant),addresses:Vec(NodeAddress)
AuditEntry = sequence:u64,unix_ns:u64,actor:ID,opcode:u16,operation:ID,
             target:Text,before_digest:Digest,after_digest:Digest,status:V
```

VOLUME_ADD-Source `0 FILE,1 BLOCKDEVICE`; UI-Textgrößen werden vor dem Aufruf zu size_bytes konvertiert.

Das Dashboard bietet die Menüpunkte **Volumes, Buckets, User, Einstellungen**. Volumes, Buckets und User zeigen jeweils einen Badge mit ihrer Anzahl; Bucket-/User-Zähler werden bereits bei Anmeldung geladen und nach lokalen Änderungen aktualisiert. Die Benutzerliste zeigt Rolle/Zustand und pro Zeile Passwortänderung und Löschen. Das eigene Konto ist vom Löschen ausgeschlossen, sowohl im Dashboard als auch bei direktem USER_DELETE. Der letzte Listeneintrag öffnet „User anlegen“ mit Benutzername und Passwort; neue Konten erhalten standardmäßig Rolle USER, optional über den Haken „Administrator“ Rolle ADMIN. In der Rollen-Spalte ist die Rolle unmittelbar per Dropdown änderbar; die Änderung verwendet USER_UPDATE (0x200c). Der angemeldete User darf sich weder selbst löschen noch die eigene Rolle ändern oder das eigene Konto deaktivieren. Das eigene Rollen-Dropdown ist sichtbar, aber deaktiviert; dieselbe Regel gilt serverseitig, unabhängig von der Zahl weiterer Administratoren. Rollenänderungen entziehen bestehende Sessions und Verbindungscapabilities des betroffenen Users; die eigene Rolle ist nicht änderbar. Eine Dashboard-Anmeldung mit korrektem Passwort, aber Rolle USER, zeigt den fehlenden Administratorzugriff ausdrücklich an. Der optionale Haken „Gleichnamigen Bucket anlegen“ öffnet nach erfolgreichem USER_CREATE das mit Name und Benutzer-ID vorbelegte Bucket-Formular. Die Bestätigung erzeugt den Bucket und gibt dem ausgewählten User Lesen/Schreiben/Anhängen/Löschen frei. Schlägt die Freigabe nach erfolgreicher Anlage fehl, bleibt dieser Teilzustand sichtbar; ein erneuter Versuch wiederholt nur die fehlende Freigabe mit derselben Request-ID. Abbrechen löscht einen bereits angelegten Bucket nicht. Die UI-Beschriftung des Hakens enthält nur „Gleichnamigen Bucket anlegen“.

Die Bucket-Ansicht zeigt Speicherklasse, Kompression, absolutes Kontingent, Freigaben und Zustand. Sie bietet Anlegen, Policy-/Kontingentänderung, Umbenennen, Sperren/Entsperren, Löschen sowie Freigaben je User mit einzelnen READ/WRITE/APPEND/DELETE-Rechten. Der System-Bucket ist schreibgeschützt in der Administration. Reine Kontingentänderungen verwenden BUCKET_SET_QUOTA ohne Datenmigration; laufende Umstellungen/Löschungen werden bis zum Abschluss aktualisiert. Der aktuelle Single-Node-Stand bietet eine Kopie bzw. EVERYWHERE auf der einzigen Node, HDD/SSD/RAM und raw/gzip/lz4/xz. Weitere Node-Replikate, garantierte Reservierungen und begrenzte physische Kontingente werden erst bei verfügbarer Server-Unterstützung angeboten. JavaScript erhält bei Listen und Policyänderungen alle u64-Werte exakt; Kontingente dürfen auch null oder kleiner als die Volume-Mindestgröße sein.

Benutzernamen: 1–128 UTF-8-Bytes, keine Leer-/Steuerzeichen oder Doppelpunkte; Präfix `node-` bleibt Node-Authentifizierung vorbehalten. Passwörter: 1–72 UTF-8-Bytes (bcrypt-Grenze), im Admin-Volume nur als bcrypt-Verifier. USER_CREATE nimmt eine nichtleere, vom Sender erzeugte User-ID und UserRole USER/ADMIN an. Benutzername und ID müssen eindeutig sein (409). Nach Passwortwechsel/Löschen werden bestehende Sessions ungültig und offene Verbindungen bei der nächsten Nachricht/Prüfung abgewiesen; beim eigenen Passwortwechsel fordert das Dashboard eine neue Anmeldung. Lokale Accountmutation und Wiederholungsbeleg werden als ein atomares Systemobjekt gespeichert; wiederholte identische Request-IDs erzeugen keine weiteren Konten oder Passwortwechsel. Belege enthalten keine Klartextpasswörter; Request-Digests sind mit dem Cluster-Secret authentifiziert. Eine künftige Cluster-Token-Rotation muss die Gültigkeit vorhandener Belege berücksichtigen. Multinode-Accountänderungen benötigen später den CONTROL_CHANGE-/EVERYWHERE-Commit.

ADMIN_GET-Section `0 ALL,1 MEMBERSHIP,2 POLICIES,3 USERS,4 GRANTS,5 GENERATION`; nicht gewählte Listen sind leer, Control/Membership-Kopf bleibt enthalten. Lokale Volume-Konfigurationen/Livezustände kommen über STATS_GET. Große AdminImages/Auditseiten werden mit limitierter Seitengröße angefragt; maximale Auditlimit 1000. User-/Admin-Passwörter werden über den Intranet-Auth-Kanal angenommen und für die Systemobjekte in Passwortverifier transformiert; der Cluster-Token und das automatisch erzeugte initiale Admin-Passwort sind die bewusst im Klartext konfigurierten Ausnahmen. Keine Passwortwerte im Audit-Log. Automatisch generierte Entity-IDs müssen beim Dedupe-Retry dieselben bleiben.

### 13.8 Dashboard-JavaScript und Codec-Prüfung

Das Dashboard baut dieselben binären Buffer mit `Uint8Array`/`DataView` auf. Jede Admin-, Stats- und benötigte Objektnachricht erhält einen benannten JS-Wrapper, z. B. `addVolume(path,class,size)`, `removeVolume(id)`, `setBucketPolicy(policy)`, `subscribeStats(filter,3000)`, `shutdownNode(id,mode)`. Wrapper geben bei Q Promises und bei F nur lokalen Queue-Erfolg zurück. `BigInt` für u64; keine stillen Präzisionsverluste durch JS-Number oberhalb 2^53. Ein zentraler Dispatcher verarbeitet RESPONSE, MAP_PATCH, STATS_SNAPSHOT und EVENT.

Ein tabellarisches, maschinenlesbares Codec-Schema und Golden Vectors müssen vor Implementierungs-Freeze die hier angegebenen OpCodes/Feldfolgen für Go/JS/C/Rust identisch abbilden. V1 akzeptiert **nur** die aufgelisteten Befehle; Race-bezogene reservierte Kommandos werden erst nach Stresstests/Algorithmusentwurf aktiviert; keine impliziten `USER_*`-/`VOLUME_*`-Wildcard-Kommandos. Unbekannte REQUEST-OpCodes erhalten 501, unbekannte PUSHes werden nur nach ausgehandelter optionaler Capability übersprungen. Kein Reflection-Marshalling interner Go-Structs als Wireformat.


### 13.9 ENet als späterer alternativer Transport

**Vorgeplant für eine spätere Version:** [codecat/go-enet](https://github.com/codecat/go-enet) als ENet-/UDP-Adapter für native Clients und Node-to-Node-Verbindungen. HTTP, administrative API und Browser-Dashboard bleiben vorhanden; ENet wird zusätzlich per Capability ausgehandelt. Die Library bindet ENet über cgo ein und benötigt die entsprechende C-Toolchain/ENet-Abhängigkeit. Sie wird optional gebaut; v1 und reine WebSocket-Deployments erhalten dadurch keine Pflichtabhängigkeit.

Der gemeinsame Objekt-/Admin-/Cluster-Codec bleibt transportneutral. Ein Transport liefert vollständige STQR-Messages an denselben Dispatcher; bei ENet entspricht eine rekonstruierte ENet-Nutzlast einer STQR-Message mit Transportumschlag, bei WebSocket weiterhin eine Binärmessage einer v1-STQR-Message. ENet-Datagramme können intern fragmentiert sein; Datagramm-/MTU-Grenzen sind keine Anwendungsnachrichtengrenzen.

#### 13.9.1 Drei Zustellungsklassen

Zustellungsklasse und **Q/F/P** sind unabhängige Achsen: Zuverlässiges F wartet weiterhin auf keine Anwendungsantwort; zuverlässiger Transportempfang ist kein Commit-/Durability-ACK. Jeder Q-Request und seine terminale RESPONSE benötigen zuverlässige Übertragung. Die Klasse ist für eine Nachricht nur zulässig, wenn ihr OpCode-/Datenvertrag dazu passt.

| Klasse | Semantik | Typische Zuordnung |
|---|---|---|
| `RELIABLE_ORDERED` (0) | Zuverlässig, geordnet innerhalb eines expliziten logischen Streams; keine zusätzliche Ordnung zwischen unabhängigen Streams | APPEND-Folgen eines Clients/Objekts, geordnete Replikationslogs, Kontrollentscheidungen, Subscription-Updates |
| `RELIABLE_UNORDERED` (1) | Zuverlässig, keine zugesicherte Zustellreihenfolge gegenüber anderen unabhängigen Nachrichten; Zuordnung über Request-/Transfer-ID | GET und Antworten, voneinander unabhängige PUTs, Migration-Chunks mit Offset/PartIndex |
| `UNRELIABLE_LATEST` (2) | Best-effort ohne Nachlieferung alter Stände; pro Zustandsstrom zählt nur die höchste gültige Sequenz | Dashboard-Metriken, periodische Zustandsanzeigen und spätere Heartbeat-Telemetrie |

- APPEND-Streams werden nach Session, Bucket und Objekt unterschieden; mehrere Clients erhalten weiterhin die totale Objektordnung vom Owner. Eine Reihenfolge zwischen getrennten Client-Streams wird nicht durch ENet erfunden. PUT/PUT_RANGE/CAS dürfen nur dann als unabhängig versendet werden, wenn keine vom Client gewünschte Abhängigkeit zu vorherigen Operationen besteht; andernfalls derselbe geordnete Stream bzw. explizite Precondition.
- Migration-Chunks dürfen umgeordnet eintreffen. COPY_END/TRANSFER_END ist eine **Vollständigkeitsbarriere**: Trifft sie zuerst ein, wartet der Empfänger auf alle referenzierten Teile und prüft Länge/Digest, statt vorzeitig zu committen. Gleiches gilt für gestreamte unabhängige PUTs; Chunk-Übertragung und atomare Objektveröffentlichung bleiben getrennt.
- Nur ersetzbare Telemetrie darf UNRELIABLE_LATEST nutzen. **STATS_FRAGMENT für die Koordinatorbildung**, REBUILD_NEEDED, Membership-/Plan-/Cutover-Belege, Objektmutationen und Subscription-Deltas bleiben zuverlässig. Ein ähnliches Stats-Payload macht einen verbindlichen Planungsbeleg nicht zu verlierbarer Metrik.
- Ein Heartbeat-Verlust oder ein ENet-Transport-Timeout löst keinen automatischen Membership-Ausschluss, Rebuild oder Ownership-Wechsel aus. STQR wartet/reconnectet gemäß §3.5; der Transport darf eine Verbindung verlieren, die Clusteridentität bleibt bestehen.

#### 13.9.2 Abbildung auf ENet

ENet liefert zuverlässige, **pro Channel geordnete** Pakete; getrennte Channels sequenzieren unabhängig. Die Go-Bindings weisen ausdrücklich darauf hin, dass `PacketFlagUnsequenced` nicht mit zuverlässigen Paketen unterstützt wird. **RELIABLE_UNORDERED ist daher eine STQR-Zustellungsklasse, kein behaupteter nativer ENet-Flag-Modus.** [ENet-Channel-Modell](https://github.com/lsalzman/enet/blob/master/docs/design.dox), [go-enet-Paketflags](https://github.com/codecat/go-enet/blob/master/packet.go).

- RELIABLE_ORDERED verwendet zuverlässig gesendete Pakete mit stabiler Stream-/Channel-Zuordnung und expliziter Streamsequenz. Channel-Wiederverwendung oder Reconnect darf keine alte Streamsequenz als neue ausgeben.
- RELIABLE_UNORDERED wird zunächst über einen **begrenzten Pool unabhängiger zuverlässiger Channels** geplant. Requests/Chunks tragen eigene IDs/Offsets und dürfen auf verschiedenen Channels unabhängig abgeschlossen werden. Innerhalb eines gemeinsam genutzten Channels kann weiterhin Head-of-Line-Blocking auftreten; es wird keine unbegrenzte Channelzahl oder vollständige Isolation jedes Requests versprochen. Ob eine spätere strengere Realisierung zusätzliche STQR-ACK-/Retransmit-Logik benötigt, ist vor Implementierung anhand Last-/Loss-Tests zu entscheiden.
- UNRELIABLE_LATEST verwendet einen von zuverlässigem Bulkverkehr getrennten Telemetriepfad und eigene Sequenzierung pro Zustandsstrom. Der Sender fasst noch nicht versandte Stände zusammen; der Empfänger verwirft veraltete/duplizierte Sequenzen. Neue BootID/Session markiert einen neuen Sequenzraum. Ein verlorener letzter Stand kann bis zur nächsten Meldung fehlen; „latest“ garantiert keine Zustellung.
- Mehrteilige Statistik-Snapshots werden nur **vollständig validiert** publiziert. Neuere Snapshots dürfen unvollständige ältere ersetzen, aber niemals zu Mischständen kombiniert werden. Letzter vollständiger Stand bleibt gegebenenfalls als STALE sichtbar. Fragmentierungsflags/-grenzen müssen gewährleisten, dass ein zu großes Telemetriepaket nicht unbeabsichtigt in eine zuverlässige Sendung umgewandelt wird.
- WebSocket erfüllt RELIABLE_ORDERED innerhalb seiner Verbindung. RELIABLE_UNORDERED behält dort die asynchrone ID-basierte Anwendungssemantik, aber das TCP-Head-of-Line-Blocking bleibt. UNRELIABLE_LATEST lässt sich vor dem Senden zusammenfassen und beim Empfangen filtern; bereits gesendete TCP-Bytes sind dadurch nicht unzuverlässig oder verwerfbar. Diese Transportunterschiede werden beim Capability-Handshake offengelegt.

#### 13.9.3 Transportumschlag, Authentifizierung und Wiederaufnahme

Für ENet wird ein **separat versionierter Transportumschlag** vorgesehen; der v1-Envelope aus §13.2 wird nicht still erweitert:

```text
DeliveryEnvelope (spätere Transportversion):
  transport_version:u8
  session_id:ID
  delivery_class:u8
  logical_stream:ID
  stream_sequence:u64
  message:Blob             # vollständige STQR-Message einschließlich ihrer Request-ID
```

Diese Feldfolge ist die Vorplanung, noch kein aktiviertes v1-Wireformat. Stream-IDs sind sitzungsgebunden, unabhängig von der begrenzten ENet-Channel-ID; Sequenzen werden pro Richtung/Stream vergeben. ENet-Session-Bootstrap wird an die bestehende Intranet-HTTP-Authentifizierung gebunden, sodass dieselben Rollen und Bucket-Rechte gelten. Kein zusätzliches Zertifikats-/TLS-Modell. Session-/Tokenbindung und Replay-/Reconnect-Regeln für UDP bleiben vor ENet-Freigabe zu spezifizieren.

Ein Transport-ACK bestätigt Empfang, niemals durable Speicherung. Nach Reconnect/Transportwechsel laufen unbekannte Mutationen mit derselben STQR-Operations-ID weiter; ENet-Sequenzen ersetzen weder Dedupe noch das Kontrolljournal. Ordered-Streams benötigen vor Freigabe einer neuen Session eine definierte Wiederaufnahme-/Abbruchbarriere, damit alte und neue Verbindungen keine gegensätzlichen Reihenfolgen erzeugen. Subscription-Handoff bleibt auf der Objekt-/Cursor-Ebene aus §10.7.


---

## 14. Clientbibliotheken und Frontends: alles sind Objekte

### 14.1 Gemeinsame Semantik

Der native Store kennt ausschließlich Buckets und 256-Bit-Objekte samt vollständigen/ranged Reads/Writes, APPEND, TRUNC und CAS. FUSE-, S3- und Blockdevice-Libs **kodieren ihre Metadaten in geeigneten normalen Objekten** und verwenden dafür dieselben GET/PUT/CAS-Operationen wie für Nutzdaten. Keine gesonderte MDS-, Namensindex-, Manifest- oder S3-Metadaten-API im Daemon.

Die Clients besitzen eine segmentierte adaptive Map und lernen über 301/Map-Patches (§6.4). Ein bekannter beliebiger Kontakt genügt; keine vollständige Map oder Agent-Installation zum Start nötig. Langlebige Clients können relevante Patches vorladen. Zustands-/Servernamenauflösung ist Protokollhilfe, keine privilegierte fachliche Metadatenengine.

### 14.2 Go-SDK

```go
conn, err := client.Dial(ctx, "http://192.0.2.10:7419",
    "app-user", credential, bucketID)
// Fehlerbehandlung ausgelassen; bucketID ist eine bekannte 128-Bit-ID.
defer conn.Close()
info, err := conn.Put(ctx, objectID, payload, client.PutOptions{})
app, err := conn.Append(ctx, logObjectID, logRecord,
    client.AppendOptions{RequestID: client.NewRequestID()})
```

**URL, Username, Passwort und Bucket-ID sind Pflichtparameter des Konstruktors.** Die erzeugte Verbindung kann ausschließlich Objekte innerhalb dieses einen Buckets lesen/schreiben: `Get`, `GetRange`, `Put`, `PutRange`, `Append`, `Trunc`, `Delete`, `Stat`, `CAS` und deren nötige `io.Reader`-/`io.Writer`-Varianten. `Close` beendet sie. `CheckFilesystem(ctx)` ist der zusätzliche Offline-Wartungseinstieg für einen dedizierten Dateisystem-Bucket und erfordert READ+DELETE; die dafür verwendete separate Verbindung bleibt bis `Close` exklusiv. Keine globale Multi-Bucket-Verbindung, kein `Bucket(name)`-/`BucketByID`-Umschalten, keine zweite `GetObject(bucket, ...)`-API und keine Admin-/Statistikfunktionen am öffentlichen Verbindungsobjekt. Eine andere Bucket-ID erfordert eine andere Verbindung. Der FUSE-/S3-/Blockdevice-Layer erhält eine bereits gebundene Objektschnittstelle und speichert eigene Metadaten in diesem Bucket. Benutzerzugangsdaten erlauben keine Administration.

Native 256-Bit-Objekt-IDs werden unverändert übertragen. Eine fachliche Namens-/Key-Abbildung ist Aufgabe des Frontends, keine zusätzliche Server-Namespace- oder SDK-Bucketauflösung. Routing/partielle Maps, Verbindungswiederverwendung, korrelierte Requests, endliche Sende-/Empfangsbudgets, Streaming und Request-ID-Retry bleiben interne Protokollaufgaben. Ein Peer-Ausfall löst keine Membership-Änderung aus; nur der aufrufende Context beendet wartende Aufrufe. Nach unbekanntem Mutationsergebnis wird dieselbe Request-ID weiterverwendet. Ein Redirect darf Zugangsdaten nicht ungeprüft an beliebige HTTP-Ziele weiterreichen.

Der aktuelle Single-Node-Transport kann bereits HELLO, Objekt-Wireformate, Chunk-/Digest-geprüfte Transfers und Reconnect/Retry. Der Daemon implementiert in diesem Volume-Abnahmeschritt noch keine Bucketverwaltung bzw. native Objekt-Dispatcher: entsprechende Requests erhalten ausdrücklich 501. Multinode-Map-Patches, Ownership-/Runtime-Lernen und Replica-Routing bleiben markierte Erweiterungspunkte statt erfundener Erfolgs-ACKs.

### 14.3 C-/Rust-Anbindung und Blockdevice-Library

- C erhält eine versionierte Handle-ABI mit expliziten Ownership-/Bufferregeln; Rust native Wrapper/async-Anbindung. Ob C auf Go-c-shared oder einen eigenen Codec setzt, bleibt eine Implementierungsentscheidung.
- Die Blockdevice-Library ist ein Satz von Dateien im Go-Package `client` und später über die C-ABI einbindbar. Oberfläche: `ReadAt`, `WriteAt`, `Flush`, `Trim`, `Resize`, Größen-/Fehlerabfrage; später QEMU-Blockdriver, optional NBD-Adapter derselben Library.
- Image = in normalen Objekten gespeichertes Manifest plus nummerierte **1-MiB-Datenobjekte**. Kleinere 512-B-/4-KiB-Blöcke verwenden GET_RANGE/PUT_RANGE; Grenzübertritte werden aufgeteilt. Ein 1-MiB-Objekt ist keine Mindest-I/O-Größe.
- Manifeständerungen verwenden CAS. Verteilte partielle Updates erhalten die native Atomizität pro Objektoperation; mehrteilige Writes über mehrere Objekte sind keine native Multi-Object-Transaktion. `FLUSH` reicht bis zum Durable-ACK aller zuvor bestätigungspflichtigen Mutationen; `TRIM` und Vergrößerung müssen Null-Leseverhalten und physische Freigabe korrekt umsetzen.

### 14.4 FUSE auf mehreren Hosts

- Dasselbe Dateisystem soll **gleichzeitig auf mehreren Nodes/Hosts mountbar** sein, etwa für CDN-/Webserver-Replikation. Es gibt keine v1-Single-Writer-Einschränkung und keinen globalen Single-Writer-Lock.
- Inhalte und FS-Metadaten werden in unabhängig adressierten Blöcken von **höchstens 1 MiB je STQR-Objekt** kodiert; größere Verzeichnisse/Metadatenstrukturen werden durch die Client-Lib auf mehrere Blöcke aufgeteilt. Für Dateiinhalte gilt dieselbe Grenze; Teilzugriffe sind ranged, größere Dateien werden gesplittet. Der letzte Chunk kann kürzer sein. Dateigröße, Inodes, Verzeichnisse, Namen, Rechte und Linkcounts sind vom Client kodierte normale Metadatenobjekte.
- Parallele Änderungen an einem Directory-/Inodeobjekt verwenden CAS mit gelesener Version und Konflikt-Retry. Direktes GET-Modify-PUT ohne Precondition ist für solche Metadaten unzulässig.
- Übergangsweise räumt `stqr-fuse --fsck` unerreichbare Objekte eines ausschließlich als Dateisystem verwendeten Buckets auf und beendet sich ohne Mount. Alle Mounts müssen geschlossen sein. BUCKET_MAINTENANCE erfordert READ+DELETE, verweigert bei weiteren Bucket-Sockets und sperrt neue Verbindungen, fremde HTTP-Objektzugriffe, Referenzen erzeugende Writes und Bucket-Konfigurationsänderungen. Die eigene Sitzung darf lesen, scannen, CAS zur Intent-Recovery ausführen (mit WRITE-Grant) und löschen. Ihre Freigabe folgt auf das Ende der Requests beim Disconnect; nach Verbindungsverlust kein automatischer Reconnect mit altem Markierungsstand. BUCKET_SCAN führt Kopien vor Tombstone-Filterung zusammen; sein exklusiver Hash-Cursor berücksichtigt auch Tombstones, weshalb eine Seite leer sein und trotzdem einen Folgecursor haben kann. Keine neue Anwendungsschema-/Namensindex-API im Storage.
- Fsck führt zunächst Root-erreichbare Namespace-Intents zu Ende und nimmt erst danach den Objektbestand auf. Anschließend validiert es den gesamten Root-erreichbaren Graphen einschließlich Verzeichnissen, Indexen, Payloads, geteilten Segmenten und Sparse-Spannen. Bei fehlenden/beschädigten Referenzen wird vor dem Löschen abgebrochen. Danach löscht es unreferenzierte Objekte mit der gescannten ExpectedVersion. Unterbrochene Sweeps sind durch einen neuen vollständigen Lauf fortsetzbar. Native Tombstones/Dedupe-Receipts bleiben erhalten; entfernte Nutzlastbytes bedeuten keine Verkleinerung der Volume-Datei.
- Ansatz für spätere Online-Freigabe, noch nicht implementiert: allgemeine objektbezogene `Open`-/`Close`-Handles plus aufgeschobener Löschwunsch (`DeleteWhenUnused`). Weitere Leser dürfen ein vorgemerktes Objekt benutzen; beim Übergang auf null Handles müssen Nullprüfung und Löschung atomar gegen neue Opens erfolgen. Der Storage kennt keine Dateisystemstruktur. Der Client bestimmt, welche Inode-/Index-/Datenobjekte ihre letzten persistenten Referenzen verloren haben. Normaler Read benötigt keine persistenten Kommunikationswrites. Crash-/Disconnect-Semantik und Replikation der RAM-Handles sind vor Freigabe festzulegen; fsck bleibt als Bereinigung nach Ausfällen verfügbar.
- Operationen über mehrere Metadatenobjekte, etwa Rename zwischen Verzeichnissen, benötigen **clientseitige Intent-/Commit-Objekte** mit idempotenter Hilfe/Wiederaufnahme, die ebenfalls über GET/PUT/CAS gespeichert werden. CAS allein verspricht keine Multi-Object-Atomicität; die genaue FS-Sichtbarkeits-/Recovery-Semantik muss vor Freigabe solcher Operationen feststehen. Im Go-Frontend verwenden Rmdir sowie Rename mit Ersetzen/Verzeichniswechsel dauerhaft entschiedene Multi-Inode-CAS-Intents: Teilnehmer in OID-Reihenfolge markieren, gemeinsame COMMIT-/ABORT-Entscheidung persistieren, Marker durch die gewählten Inodebilder ersetzen. Andere Zugriffe helfen unterbrochene Operationen zu Ende; reine Leser ohne WRITE dürfen das alte committed Bild bei PENDING bzw. das neue bei COMMITTED lesen. Entfernte Verzeichnisse erhalten einen persistierten Removed-Zustand gegen Creates über alte Handles. Verzeichnis-Moves prüfen und schützen den Zielvorfahrenpfad gegen Zyklen. Live-Inodes bleiben v1, entfernte Verzeichnisse verwenden v2; Marker/Intents setzen ein gemeinsames Upgrade aller Mounts voraus.
- `fsync` bestätigt erst nach nativer Persistenz. TRUNC verkürzt den letzten Chunk und entfernt überzählige Chunks über einen publizierten Metadatenstand. **Bis SUBSCRIBE implementiert ist, sind für Tests alte/stale Client- und Kernel-Caches ausdrücklich ausreichend**; die Testversion verspricht deshalb keine sofortige Sichtbarkeit zwischen Mounts. Mit SUBSCRIBE muss der Client konsistent arbeiten: Snapshot/Updates, Versionsprüfung, Invalidierung und Resync müssen veraltete Cacheantworten verhindern. Die atomaren Storage-/CAS-Regeln gelten bereits davor. POSIX-Locks, mmap-Schreibkohärenz und alle POSIX-Randfälle sind gesondert zu spezifizieren, nicht durch „Mehrhost-Mount“ automatisch zugesichert.

#### Native Readonly-/Overlay-Mounts

`stqr-fuse --readonly-bucket ID` mountet ausschließlich lesend; `--bucket ID` mountet ein normales RW-Dateisystem. Beide Optionen zusammen verwenden den ersten Bucket als Lower und den zweiten als privaten Upper. Separate `--readonly-uri`, `--readonly-username` und `--readonly-password` erlauben eine entfernte gemeinsame Basis und einen lokalen Upper; ohne Overrides gelten URI/Credentials der RW-Verbindung. Jeder Bucket erhält eine eigene vollständig authentifizierte Client-Verbindung. Readonly setzt zusätzlich Kernel-`ro` und unterbindet clientseitig alle Mutationen einschließlich Intent-Hilfe; reine Reads benötigen keine persistierten Kommunikationswrites. `--allow-other` ermöglicht bei entsprechender Host-FUSE-Konfiguration weitere lokale UIDs, ohne Unix-Dateirechte aufzuheben.

Upper verwenden einen eigenen Root-Marker `STOR` v1 mit Lower-Bucket-ID und einer segmentierten `STOM`-Manifeststruktur. Als Overlay formatierte Buckets sind ohne passende Basis nicht mountbar. Das kanonische Manifest enthält sparse Directory-Records (Dir-OID, Lower-Dir-OID, Opaque-/Metadaten-Flags, Inodeattribute, sortierte Namenseinträge). OID=0 im Namenseintrag ist eine Löschvormerkung. Neue/neu angelegte Verzeichnisse sind opaque und lassen zuvor verdeckte Lower-Inhalte nicht wieder erscheinen. Ein Root-CAS veröffentlicht Änderungen mehrerer Overlay-Verzeichnisse atomar; normale Dateiänderungen verwenden weiterhin unabhängige Inode-CAS. Der derzeitige Sparse-Manifest-Editor materialisiert maximal 64 MiB und schreibt das Manifest bei Namespaceänderungen neu; dies ist auf kleine lokale Konfigurations-/Upload-Overlays ausgelegt.

Metadaten-Copy-up speichert nur Attribute und einen Lower-Inode-Verweis mit erwarteter Version. Teiländerungen bestehen aus eigenen Datenobjekten und Referenzen auf unveränderte Lower-Bereiche. STFS v3 erweitert den Inode um Lower-Verweis/Version und autorisierten ursprünglichen Schreibpfad; STFS v4 ergänzt persistiertes atime/ctime. STIX v2 besitzt 64-Bit-Quelloffsets und je Eintrag eine optionale Lower-Inode-Version, sodass auch große/sparse Bereiche ohne Nutzlastkopie darstellbar sind. Ein geänderter referenzierter Lower-Inode ergibt ESTALE statt still vermischter Inhalte. Referenzierte Basisdateien müssen daher stabil bleiben; automatische Rebase-/Migrationslogik für andere Basisversionen ist nicht Teil dieser Implementierung. Unveränderte Lower-Dateien ohne Upper-Kopie bleiben durchgereicht.

`--whitelist=conf.json,fop-files/` erlaubt ausschließlich die Datei relativ zur Mount-Wurzel sowie den ganzen angegebenen Verzeichnisbaum. Existierende Lower-Verzeichnisse werden auch ohne abschließenden Slash erkannt. Keine Globs, absoluten Pfade oder Punkt-/Parent-Komponenten; neue Verzeichnisregeln benötigen einen Slash. Die kanonische Liste wird im Upper persistiert: Weglassen übernimmt die gespeicherte Liste, eine abweichende explizite Liste wird abgewiesen. Beide Seiten von Rename/Exchange sowie tatsächliche Schreib-/Metadaten-/Löschoperationen prüfen die Liste. Atomare Save-Dateien müssen ebenfalls in einem erlaubten Bereich liegen. Die Frontend-Whitelist ist keine Isolation gegen direkte SDK-Operationen mit RW-Bucket-Credentials und keine Prozess-Sandbox.

`--fsck` mit beiden Bucket-Optionen sperrt exklusiv nur den Upper, prüft dessen sichtbaren Graphen und alle verwendeten Lower-Versionen, entfernt unerreichbare Overlay-Directory-Records und sammelt unreferenzierte Upper-Objekte. Es löscht niemals Lower-Objekte. Die geöffnete reine Lower-Verbindung verhindert gleichzeitig eine Lower-Bereinigung während dieser Prüfung. Fehlerhafte/veraltete Basisreferenzen brechen vor dem Sweep ab. Automatische Online-Speicherfreigabe bleibt ausstehend. Nach abgeschlossener Lower-Wartung entfernt Upper-fsck redundante Whiteouts, deren Name im aktuellen Lower-Verzeichnis fehlt, sowie Whiteouts in opaque Verzeichnissen. Bereits im Lower vorhandene Namen bleiben maskiert; nach Entfernung eines redundanten Whiteouts wird ein später neu angelegter Lower-Name wieder sichtbar. Währenddessen keine parallelen Lower-Writes, da die Upper-Wartungssitzung den Lower nicht einfriert. Unerreichbar gewordene, aber nicht als entfernt markierte Upper-Verzeichnisrecords werden bei Lower-Verzeichniswechseln als ESTALE gemeldet und mit ihren lokalen Daten erhalten. Vollständiges Rebase und das Erfassen offline gespeicherter Upper-Abhängigkeiten durch Lower-fsck sind weiterhin offen; diese Fälle dürfen nicht als automatische Reparatur versprochen werden. Readonly-Opens erlauben mmap für PHP; PHP-`LOCK_EX` funktioniert innerhalb desselben Mounts über lokale Kernel-Locks, ohne ein verteiltes Lock-Versprechen. `touch` kann atime/mtime ausdrücklich setzen; normale Reads persistieren atime nicht.

### 14.5 Eigene S3-Library und lokales Gateway

- Die S3-Library im Package `client` implementiert den zugesagten S3-Umfang auf nativen Objekten. Ein lokaler HTTP-Gateway-Adapter ist ein optionaler Betriebsmodus desselben Daemon-Binaries, kein zusätzlicher Admin-Dienst.
- Objektinhalte, Key-/Prefix-Index, ETags, Multipart- und Uploadzustände sind normale STQR-Objekte. LIST liest einen vom S3-Client gepflegten Index; der Storage-Server bietet keine native lexikographische Listing-Engine.
- Das erste S3-Frontend-Release umfasst **PUT/GET/HEAD/DELETE, LIST (einschließlich Prefix/Pagination) und Multipart (Initiate/UploadPart/ListParts/Complete/Abort)**. Keyindex und Uploadzustand liegen in normalen Objekten; Konflikte werden über CAS-/Intent-Objekte aufgelöst. Vor SUBSCRIBE darf das Testfrontend den vereinbarten veralteten Cachestand nutzen, danach gilt konsistente Aktualisierung. S3-Signatur-/Credential-Mapping muss native Bucket-Rechte erhalten.
- Standardmäßig Bind an localhost; Remote-Bind explizit auf das vorgesehene Intranet. Ein verteiltes Deployment kann mehrere Gatewayinstanzen auf denselben Buckets betreiben, sobald deren Metadatenkonflikte korrekt über CAS gelöst sind.

### 14.6 Native Frontends als verbindlicher Umfang

FUSE, S3 und Blockdevice werden direkt auf STQR-Objekten implementiert und für dessen ranged I/O, CAS, Sparse-Extents, Kompression und spätere Subscriptions optimiert. Es wird keine Fremd-Storage-Drop-in-Schicht und keine spätere entsprechende Kompatibilitäts-Roadmap geführt.

---

## 15. Admin-Dashboard und operative Bedienung

### 15.1 Aufbau

- Das Dashboard ist im HTTP-Daemon eingebettet und verwendet nach Basic-Login den gemeinsamen binären WebSocket-Codec (§13). Kein separater Webadapter oder Dashboard-Daemon. Administrative API und Dashboard rufen dieselben Handler auf.
- Login als `ADMIN` erforderlich für Änderungen; nur berechtigte Accounts dürfen Statistiken sehen.

### 15.2 Hauptansicht: Nodes und Volumes als Baumtabelle

Beispiel:

| Node / Volume | Class | Storage gebraucht | Frei | Read IOPS | Write IOPS | Read MB/s | Write MB/s | P99 Read | P99 Write | Status |
|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---|
| ▾ Node-A | — | 1.2 TB | 0.8 TB | 42k | 11k | 850 | 210 | 3 ms | 9 ms | healthy |
| ↳ nvme0n1 | SSD | 700 GB | 300 GB | 37k | 10k | 720 | 190 | 2 ms | 7 ms | active |
| ↳ disk.img | HDD | 500 GB | 500 GB | 5k | 1k | 130 | 20 | 8 ms | 18 ms | active |
| ↳ RAM cache | RAMCACHE | 20 GB | 44 GB | 12k | 0 | 120 | 0 | 0.2 ms | — | active |

**Echtzeit-Ticker:** Einmaliges STATS_SUBSCRIBE, danach sofortige und alle **3 Sekunden** gepushte Snapshots desselben Collectors wie für den Generationen-Builder (§7.1). Delta-Zähler ergeben IOPS/Throughput, physische Belegung wird nicht mit logischer Bucket-Nutzung verwechselt. Veraltete/offline Daten bleiben markiert sichtbar. Kein externer Zeitreihendienst für den Datenpfad nötig.

### 15.3 Admin-Menüs

- **Nodes:** Mitgliedschaft, Join-/Leave-Status, Fencing-/Quorum-Health, Failure Domains, Version.
- **Volumes:** Baumstruktur, Online-/Draining-/Unmounted-Zustand, Class, Mount-Pfad, Kapazität, SMART/IO-Fehler, Add/Remove.
- **Buckets:** anlegen, löschen, konfigurieren; Replikationszahl, EC-Profil, Basis-Storage-Class, Read-Replica-/RAMCACHE-Budget, Quotas, Zugriffsrechte; `CURRENT` vs. `DESIRED`/`NEXT` anzeigen.
- **Bucket-Statistiken:** logische/physische Bytes, Objektzahl, Reads, Writes, Appends, Top Hot-Ranges, Cache Hit Rate, Durable-Copies, EC-Overhead, Policy-Erfüllung.
- **Generationen:** CURRENT/NEXT, Rebuild-Gründe, gewählter Koordinator, Plan-Kosten, bytes moved/remaining, Migrationsstatus je Range, ETA nur als geschätzte Metrik, Quorum-Fortschritt.
- **User:** Admin/User, Bucket-Freigaben, Credential- und Token-Rotation, Audit-Log.
- **Alarme:** nearfull, full, lost quorum, degraded replicas, EC below threshold, stale owners, failed migrations, unsafe hardware flush, hot I/O.

### 15.4 Administrative Bedienung

Dashboard-JS-Wrapper erzeugen binäre Nachrichten nach §13.7. Skripte verwenden HTTP Basic plus `/api/message` oder die JSON-Volume-Komfort-API. Ein Volume wird in **einem** Add-Aufruf validiert, gegebenenfalls formatiert, gemountet und konfiguriert. Neue Nodes starten ohne init-/join-CLI-Unterbefehle über die Daemon-Konfiguration bzw. `--connect`. Es gibt kein separates Administrations-CLI und keinen lokalen Map-Agent.

---

## 16. Security- und Betriebskonzept

### 16.1 Rollen und Secrets

- `CLUSTER_TOKEN`: berechtigt zur **Node-Rolle** und Node-to-Node-Funktionen, nicht zu normalen User-Sessions über einen alternativen unauthentifizierten Pfad.
- HTTP Basic Auth bzw. daraus erzeugte Browser-Session: Client- oder Admin-Rolle; **Rolle und Rechte werden auf dem Server ermittelt**, nicht vom Client behauptet.
- Bucket-Freigaben werden auch auf Storage-Nodes geprüft; SDK-Checks alleine reichen nicht.
- Credentials: initialer Admin und Cluster-Token im Klartext in lokaler Config, weitere User-/Admin-Verifier in EVERYWHERE-Systemobjekten (§5.7).
- Token-Rotation: neue/alte Cluster-Token-Version in einer kontrollierten Übergangsphase; kompromittierter Token ist ein Incident, der Node-Impersonation ermöglichen kann.
- Admin-Aktionen werden mit Actor, Timestamp, alter/neuer Policy, Generation und Outcome auditiert.

### 16.2 Isolation

- Bucket-spezifische Namespaces, Quotas, Backpressure und I/O-Scheduling verhindern, dass ein Kunde unbegrenzt den gesamten Cluster blockiert.
- Objekt-Checksummen und optional AEAD-Verschlüsselung-at-Rest; wenn vorgesehen, Schlüsselmanagement getrennt vom Volume-Datenformat.
- Der Admin kann Bucket-Policies verwalten, ohne die direkten Client-I/O-Pfade zu verändern.

### 16.3 Hardware-/Datenfehler

- Checksummen pro Record/Segment; Scrubbing zur Erkennung stiller Korruption.
- Replikate/EC-Fragmente zum Reparieren verwenden; keine Reparatur aus unvalidierten Read-Caches.
- Volume-Format und Recovery-Werkzeuge sind unabhängig von der Distribution verfügbar.
- Disk full, Journal full, GC-Stall und failed flush müssen explizite Fehler liefern und dürfen nicht als `SUCCESS` interpretiert werden.

---

## 17. Go-Projektstruktur und Entwicklungsregeln

Aktueller Implementierungsauftrag: Volume-Library plus Single-Node-Daemon, integriertes Admin-Dashboard und Tests; kein separates Admin-CLI. Der Gesamtaufbau sieht ein Daemon-Binary `stqr` im Root und **maximal drei Library-Packages**: `volume`, `server`, `client`. Der FUSE-Client ist ein separates Binary `stqr-fuse` unter `cmd/stqr-fuse`; er erhält URI, Username, Passwort, Bucket-ID und Mountpunkt als Parameter und verwendet die Client-Library. Der Linux-Adapter verwendet `go-fuse` für das FUSE-Kernelinterface. Keine `pkg/`-/`internal/`-Hierarchie mit einem Package pro Einzelkomponente, kein separates Admin-CLI/Agent/Dashboard-Binary.

```text
stqr/
  go.mod
  main.go                  # Daemon, --connect, Configstart
  cmd/stqr-fuse/           # separates FUSE-Client-Binary; URI, Credentials, Bucket, Mountpunkt
  volume/                  # Go-Library; nicht das Datenverzeichnis volumes/
    volume.go              # Datei/Raw-IO, Lifecycle und physische Admission
    format.go              # Superblock, Record-/Extent-Codierung, Checksums
    index.go               # Index, Checkpoint und Replay
    arena.go               # Append-Arenen und Blockliste
    gc.go                  # Compaction und Freigabe
    *_test.go
  client/
    client.go              # öffentliche Objekt-API, Routing, Futures/Retry
    protocol.go            # gemeinsamer öffentlicher Codec und Wire-Datentypen
    transport.go           # transportneutrale Zustellung; WebSocket v1, ENet später optional
    stats.go               # Statistik-DTOs/Codec, keine Servermessung
    map.go                 # segmentierte Client-Map
    blockdevice.go         # Image-/Chunk-/Manifest-Library
    filesystem.go          # CAS-basierte FS-Metadaten, FUSE-Anbindung
    s3.go                  # S3-Objekt-/Index-Library und Gateway-Handler
    *_test.go
  server/
    server.go              # HTTP/Basic, gemeinsamer Message-Dispatcher
    transport.go           # WebSocket, später optional ENet im selben Package
    config.go              # lokale Node-/Volume-/Peer-Konfiguration
    cluster.go             # Membership, Kontrolljournal, Koordinatorwahl
    planner.go             # Statistikaufnahme, physischer Zielplan
    migration.go           # Copy/Catch-up/Fencing/Cutover
    replica.go             # ALL-Replica-Commit und Recovery
    objects.go             # API-/Volume-Adapter, Serialisierung/Dedupe
    stats.go               # Collector und Dashboard-/Peer-Subscriptions
    dashboard.go           # eingebettete Assets, Sessions
    dashboard/             # HTML/JS/CSS, kein Go-Unterpackage
    *_test.go
  bindings/                # C-/Rust-Quellen, keine weiteren Go-Library-Packages
  testdata/                # Golden Vectors, Wire-/Recovery-Fixtures
  docs/                    # zusätzliche Format-/Betriebsdokumente bei Bedarf
```

Importgraph: Root -> server/client; server -> client/volume; client und volume -> Standardbibliothek bzw. externe Libraries. `client` ist unabhängig von server/volume. `volume` kennt keine HTTP-/Cluster-Datentypen; server adaptiert lokale Storage-Requests. Gemeinsam benötigte öffentliche Wire-Typen liegen in client, ohne dort Serverlogik einzuziehen. Tests im jeweiligen Package bzw. server-Integrationstests, keine Importzyklen.

Die FUSE-/S3-/Blockdevice-Dateien sind fachliche Libraries im Package client. Optionale schwere Plattformadapter können per Build Tags aktiviert werden; neue Modul-/Packagegrenzen nur bei tatsächlich nachgewiesenem Bedarf und innerhalb der drei Library-Packages. Dashboard-JS benutzt denselben dokumentierten Codec, keine Go-Imports.

`context.Context` begrenzt einzelne Aufrufe, nicht automatisch Membership-Lebensdauer. Buffer-/Queue-/Staging-Budgets sind endlich. Persistente und Wire-Strukturen sind versioniert, deterministisch serialisiert und unabhängig vom Go-Speicherlayout. Strukturierte Logs enthalten keine Secrets.

---

## 18. Tests und formale Invarianten

### 18.1 Unverzichtbare Invarianten

1. Jede gültige Objektadresse gehört in jeder committed Map **genau einer** logischen Range je Bucket.
2. Ranges sind vollständig, lückenlos, monoton nach `start` sortiert und beginnen bei `0`.
3. Jede writable Range hat **höchstens eine gültige Schreibautorität** für ihre Ownership-Epoch; eine neue Epoch wird erst nach persistentem Source-Fencing oder administrativ bestätigtem externem Fencing aktiviert.
4. Ein als erfolgreich bestätigter HDD-/SSD-Write/Append bleibt nach Crash im bestätigten Durability-Modell rekonstruierbar. RAM- und spätere CACHE-Buckets folgen ausdrücklich ihrem flüchtigen Vertrag (§8.1).
5. Zwei parallele Appends auf ein Objekt werden **als Ganzes** in einer eindeutigen Reihenfolge sichtbar; keine Interleavings.
6. Ein wiederholter mutierender Request mit derselben Request-ID hat **höchstens einen logischen Effekt**.
7. Eine alte Objektversion oder ein Tombstone darf nach Cutover/Failover nicht wieder zum neuesten Objektzustand werden.
8. READ_CACHE zählt niemals als durable Kopie.
9. Keine unautorisierte Bucket- oder Node-Operation wird akzeptiert.
10. Ein bestätigter Plan-/Ownership-Wechsel ist nach Koordinator-Crash und Quorum-Recovery eindeutig rekonstruierbar.

### 18.2 Unit-Tests

| Modul | Fälle |
|---|---|
| Hash | Golden Vectors über Go/C/Rust, Empty-/Binary-Keys, Bucket-Trennung, direkte 256-Bit-IDs |
| Ranges | Start 0, MaxHash, Off-by-one, Split von Minimalranges, Merge, Sortierung, Lücken/Überlappungen |
| Maps | Encode/Decode, Checksum, RAM-Buffer-/Segment-Lifetime, beschädigte Maps, Concurrent Refresh |
| Planner | Byte-/IO-Balancing, Mehrheitsregel bei Merge, Hysterese, Hot Range, Failure Domains |
| Volume | Datei vs. Blockdevice-Mock, Header-Corruption, Alignment, Sync, Hotplug-State |
| Object | Vollständiges/ranged Put/Get, Delete, Trunc, APPEND-Neuanlage, konkurrierende Teilwrites, checksum, tombstones, große Objekte aus vielen Records und massive kleine Objekte |
| Quota | Guaranteed-/Absolute-Grenzen, 507 bei nicht erfüllbarer Erhöhung, Schutz fremder Reserven, paralleles Wachstum, Retry/Refund, Klassen-/Replikationsbedarf |
| Append | parallele Appends, Offset/Sequence, Request-ID-Retry, Zero-Length, MaxSize, Retry nach Timeout |
| Replica | ALL-ACK, Replica-Failures, in-order replay, exact durable commit, degraded states |
| Auth | Node-Token-Prüfung, HTTP-Basic-/Session-Auth, Replay, RBAC, Bucket-Zugriff, Admin-only |
| Protocol | Status-Varints inkl. 301/401/507, variable Folgeheader, Teilframes, Mehrfachframes, Fuzzing, Backpressure, Stream-Abbruch, Redirect-Ziele und GOAWAY beim Drain |
| Frontends | S3-Key-Handling/Listing, FUSE-1-MiB-Grenzen/fsync/truncate, Blockdevice-Library mit Sub-MiB-Zugriffen über Objektgrenzen, NBD-FLUSH/TRIM, Go-SDK-Retries |

### 18.3 Property-Based-/Fuzz-Tests

- Zufällige Buckets und Objekt-Hashes, zufällige Split-/Merge-Folgen; immer exakt ein Range-Lookup.
- Zufällige `PUT/PUT_RANGE/APPEND/TRUNC/DELETE/GET/GET_RANGE`-Operationen mit Referenzmodell, einschließlich konkurrierender Änderungen am selben Objekt.
- Zufällige Map-Änderungen während Clients mit alten Generationen Requests senden.
- Protocol-Fuzzing für alle Frame-/Handshake-Parser und persistente Record-Decoder.
- Interleavings von Replikations-ACKs, Request-Retries und Fencing.

### 18.4 Crash- und Fault-Injection

Mit künstlich kleinen Volumes (z. B. **4, 8, 16, 32 und 64 MiB**) und deterministischen Zufallsseeds:

- Crash nach jedem kritischen Persistenzschritt (Record geschrieben, Flush begonnen, Flush beendet, Index/Checkpoint, Response vor/nach Send).
- Node-Crash mitten im `APPEND`, beim Kopieren, während Catchup und unmittelbar vor/nach Cutover.
- Quelle/Ziel/Koordinator getrennt killen und neu starten.
- Packet loss, delayed/duplicated/reordered requests (auf Anwendungsprotokollebene simuliert), TCP-Disconnects, Halbverbindungen, Netzwerkpartitionen.
- Volume full, nahezu full, plötzlicher Unmount, I/O-Fehler, Bitflips, beschädigter Superblock, Checkpoint-Rollback.
- Token-Rotation und Berechtigungsentzug während laufender Sessions.
- Gleichzeitige Bucket-Policy-Änderung, Join, Leave und Hotspot-Rebuild-Gründe.

**Erfolgsbedingung:** Nach Recovery kein bestätigter HDD-/SSD-Write/Append verloren, keine Doppel-Appends, keine doppelte Primary-Autorität; Fehlermeldung bei unerfüllbarer Durability statt stillschweigender Abschwächung.

### 18.5 Last- und Stresstests (kleine Volumes)

- Objekte 2 B, 2 KiB, 4 KiB, 16 KiB, 64 KiB, 200 KiB, 250 KiB, 256 KiB, 1 MiB sowie größere native Objekte über mehrere Records/Segmente, gemischt; hohe Objektzahl auf kleinen Test-Volumes durch kurze Churn-Zyklen.
- Viele kleine parallele Appends auf **dasselbe** Log sowie viele unabhängige Logs; verifizierter sequentieller Byte-Präfix.
- 1→2→3→N Nodes, jeweils mit MiB-Volumes; Verteilung/Replikation unter Last.
- 95–99 % Füllstand, GCs und Rebalancing, neue leere Ziel-Volumes, kritische Backfill-Situation.
- Hot-Range durch 90 % Reads auf einem Schlüssel; Split-Isolation und Cache-Replikation; separate hohe Write-/Append-Last.
- Tausende kleine Buckets, viele leere Ranges, aggressive Merge-Folgen.
- Map mit simulierten Hunderttausenden Ranges (ohne große Datenvolumes) für Memory-/Lookup-Benchmark.
- Benchmark-Metriken: IOPS, Durchsatz, P50/P95/P99, CPU/Operation, System Calls, Copy Bytes, Flush/Commit-Latenz, Write Amplification, Map-Größe, Recovery-Zeit.

### 18.5.1 Zusätzliche Protokoll-/Betriebsabnahmen

- Wahl-Races: erst Mess-/Stresstestharness mit gleichzeitigen Auslösern, verzögerten/überkreuzten Fragmenten und Reconnects; Konflikte, Konvergenzzeit und Overhead messen. Erst anschließend den Race-Algorithmus entwerfen und dessen Eindeutigkeit/Crash-Sicherheit prüfen, keinen Grant-/ABORT-Algorithmus vorwegnehmen.
- Keine neue Generation ohne Grund; mehrere Gründe in needrebuild bündeln, bei ausführbaren Gründen direkt nach Finalize nächsten Lauf starten; Rebuild mit 0 Payloadbytes bei rein logischem Split/Merge.
- Online-Rebuild bei über mehrere Tage fehlenden Nodes: unabhängige Online-Ranges umverteilen und Generation abschließen; Offline-Range-Groups einschließlich Grenzen, Locations, Rollen und Epochs unverändert lassen. Offline-Volumes ebenfalls einfrieren. Aufgeschobene Gründe über Neustarts erhalten, ohne wirkungslose Folgeläufe; nach Reconnect/Abgleich in einer neuen Generation umsetzen. Während eines gebundenen Laufs ausfallende Teilnehmer müssen dessen Abschluss blockieren; wiederkehrende Nichtteilnehmer dürfen erst eine Folgerunde erweitern.
- Gemeinsame Stats-Codec-Golden-Vectors Go/JS, 3-Sekunden-Push ohne Polling, mehrteilige Snapshots, BootID-Wechsel und Replika-Accounting ohne logische Doppelzählung.
- Leere/UNKNOWN-Client-Map, zufälliger Erstkontakt, 301 mit Patch/Push, Grenzbereiche/Split/Merge/alte Patches und segmentierte Aktualisierung ohne falsche Range-Erweiterung.
- Basic-Auth-/Cookie-/Origin-Prüfung, HTTP und WebSocket mit identischen Adminhandlern, Q exakt eine terminale Antwort, F keine RESPONSE, begrenzte Queues und Stream-Abbruch ohne partiellen Commit.
- HDD-Fallback nur auf freien SSD-Platz ohne fremde Garantiereserven; SSD auf HDD => 507; Index-/Arena-/COW-Overhead kann trotz logischer Quota 507 auslösen.
- Automatischer leere-Config-/Standalone-Start, --connect wartet bei Unerreichbarkeit, ein Add-Aufruf formatiert nur das angegebene neue Ziel; beschädigtes vorhandenes Volume wird beim Neustart nicht gelöscht.
- DRAIN/SHORT/CLUSTER-Shutdown, laufende Flushes und verlorene STOP-Pushes; Forceful Disconnect nur nach bestätigtem externem Fencing, LOST statt fälschlichem NOT_FOUND.
- 10-Byte-Sync-Appends in vorreservierten Arenen, tatsächliche physische Bytes und Write-Amplification; Crash beim selben physischen Tail-Block darf keine früher bestätigten Bytes zerstören.
- RAM: PUT/APPEND/CAS, 507 statt Eviction, lokale Kopie nach Neustart leer und aus lebenden Replikaten vor Aktivierung restauriert, Totalverlust setzt Runtime zurück, alte Runtime-Retries => 410, DRAIN erhält Zustand, SHORT mit Replica-Restore und kompletter CLUSTER-Neustart mit leeren RAM-Daten; CACHE-Aktivierung v1 => 501.
- Replica-Reads: Primary offline/SHORT, erreichbare gültige Replikate liefern committed Daten ohne Primary-Warteblockade; keine vorbereiteten Records/Restore-Zwischenstände. Writes ändern dabei nicht ihren ALL-/Ownership-Vertrag.
- Sparse/Codecs: PUT_RANGE erzeugt fehlende Objekte/Null-Lücken, TRUNC vergrößert ohne physische Nullfüllung; riesige ZERO-Extents bei kleinen Volumes, teilweise Überschreibung, Crash-Replay und Migration. raw/gzip/lz4/xz mit ranged I/O, kleinen Appends und Codecwechsel, ohne logische Versions-/Offsetänderung.
- EVERYWHERE/System: Join/Leave/SHORT, vollständige Systemobjektkopie je Mitglied, automatische Config-Credentials/System-Volume, Hashablage weiterer Accounts und Authentifizierung aus erreichbaren System-Replikaten. Ungeeigneter physischer Platz bei Join => 507.
- S3/FUSE: LIST/Prefix/Pagination und Multipart-Complete/Abort im ersten Frontend-Release; Testphase darf stale Caches haben, Einführung von SUBSCRIBE benötigt danach konsistente Cache-/Update-Verarbeitung.
- Später ENet: Paketverlust/Duplikation/Reordering, isolierte Ordered-Streams, unabhängige Request-IDs, verspätete END-Nachrichten, Latest-Snapshot-Verwerfung ohne Mischstände, Channel-Erschöpfung, Reconnect mit gleichen Operations-IDs und keinerlei automatischer Membership-Ausschluss durch Transport-Timeout.
- Später: Snapshot+SUBSCRIBE ohne Registrierungslücke, cursorbasiertes Replay, Range-Updates/TRUNC/DELETE und Übernahme aktiver Subscriptions bei Cutover/Drain.

### 18.6 Langzeit- und Verifikationstests

- Kontinuierlicher Chaos-Test über viele Generationen mit Seed-Reproduktion.
- Migration großer Range-Anzahl bei intermittierenden Schreiblastspitzen.
- Rolling Upgrade/Format-Version-Mismatch.
- Scrub und Rebuild nach gezielten Datenfehlern.
- Konsistenzmodell mit History-Checker prüfen: totale Mutationsordnung pro Objekt, atomare lokale Read-Snapshots und Sichtbarkeit bestätigter Writes auf allen erforderlichen Replikaten für danach beginnende Reads. Während laufender Mutationen sind unterschiedliche Replica-Versionen und Versionsrückschritte beim Replikawechsel zulässig. Verzögerte PREPARE-/COMMIT-Zustellung darf Reads des letzten lokal committed Stands nicht bis zum Write-Abschluss blockieren.

---

## 19. Implementierungsphasen und Abnahmekriterien

### Phase 0 – Spezifikations-Freeze

- Vollständiges Wire-Format, On-Disk-Versionierung, API- und Durability-Vertrag.
- Messharness für Koordinator-Races planen; finaler Race-Algorithmus erst nach Stresstests. Unabhängig davon administrative Membershipwechsel, Ownership-Fencing und APPEND-Retry-Window spezifizieren.
- Golden-Testvektoren für Hash, Binärprotocol und Map.

### Phase 1 – Lokaler STQR-Node

- Ein Node; Regular-File-Volumes und RAM-Buckets; Superblocks, Segment-Records, Index/Checkpoint/Replay; `PUT/GET/PUT_RANGE/GET_RANGE/DELETE/APPEND/TRUNC/CAS` einschließlich HTTP-/WebSocket-Transport und Streaming, Request-ID und Group Commit sowie Guaranteed-/Absolute-Quota.
- Bucket-Admin, Basic-/Session-/User-RBAC mit automatischer Config und EVERYWHERE-Systemobjekten, integriertes Dashboard/API, gemeinsamer Stats-Collector und Go-SDK; Sparse-Extents, raw/gzip/lz4/xz und adaptive Single-Node Map.
- **Abnahme:** Crash nach jedem simulierten I/O-Schritt ohne Verlust bestätigter HDD-/SSD-Objekte/Appends; RAM lokal nach Neustart leer, Replica-Restore vor Aktivierung; nach Verlust aller Prozesse leer und während Laufzeit ohne Eviction.

### Phase 2 – Range-Engine und Hotplug

- Hash-Ranges, Split/Merge, replizierte Map noch ohne automatische Migration; zusammenhängende Server-Map, segmentierte adaptive Client-Map; Volumes live add/drain; Realtime-Statistiken.
- **Abnahme:** Splits/Merges ohne Bytes zu bewegen; korrekter direkter Client-Lookup; 95–99-%-Szenarien im kleinen Testaufbau.

### Phase 3 – Multi-Node-Cluster und Generationen

- Cluster-Join per `CLUSTER_TOKEN` + Seed-URI, Konsensmetadata, Wahl des temporären Plan-Koordinators, PREPARE/COPY/CATCHUP/CUTOVER, Stale-Map-Retry.
- **Abnahme:** Sicherer Move bei parallelen Appends/CAS und Crashes; Stresstestmessung konkurrierender Kandidaten vor Entwurf des Race-Algorithmus, Koordinator-Reconnect statt Neuwahl, needrebuild-Folgelauf und Drain erst im erstellten Plan.

### Phase 4 – Durable Replication und Balancing

- Replica N/ALL und EVERYWHERE für Disk/RAM, Failure Domains, erlaubte Replica-Reads, Recovery nach Ausfall, capacity-/load-aware Planner, hot-range isolation, Policies per Bucket über NEXT.
- **Abnahme:** Keine falschen Erfolgs-ACKs; Warten bei Ausfall, gefenceter administrativer Disconnect; null bzw. minimale Migrationsbytes bei erfüllten Constraints.

### Phase 5 – Frontends, Dashboard, Performance

- C/Rust-SDK, FUSE mit 1-MiB-Chunks, Blockdevice-Library mit 1-MiB-Objekten und kleineren ranged Transfers, lokales S3-Gateway, Admin-Dashboard mit Echtzeit-Node-/Volume-Tabelle und Bucket-Verwaltung; NBD auf derselben Library nach Bedarf, spätere QEMU-Anbindung vorbereitet.
- **Abnahme:** Multi-Mount-FUSE mit CAS-Konflikten und zunächst erlaubten Testcaches; S3 inkl. LIST/Multipart und Blockdevice-Semantik, End-to-End-Tests mit gemeinsamen Objektoperationen.

### Phase 6 – Erweiterungen

- Optionaler ENet-Transport mit drei Zustellungsklassen (§13.9), SUBSCRIBE/UNSUBSCRIBE mit Migrations-Handoff, optionale CACHE-Strategie, dynamische RAM-Read-Caches, Cache-Kohärenz, Erasure Coding, segmentbasierte EC, Quorum-ACK-Varianten, SSD-/NVMe-spezifische Fast Paths.

---

## 20. Noch offene Detailverträge und spätere Features

Hier stehen nur noch nicht entschiedene Details. Bereits geklärt sind: Intranet-HTTP ohne TLS; automatisch erzeugte Klartext-Bootstrap-Credentials in der Config; User-/Admin-Verifier in EVERYWHERE-Systemobjekten; Replica-Reads bei fehlendem Primary; PUT_RANGE mit Neuanlage/Null-Lücken; verlängerndes TRUNC; Sparse-Extents; vier Kompressionsverfahren; LIST und Multipart; Testcaches bis SUBSCRIBE, danach Konsistenz; ausschließlich native Frontends.

### 20.1 Persistenz, Recovery und Koordinator-Races

- Zuerst reproduzierbare Stresstests/Messungen konkurrierender Koordinatorangebote und Statistikeinsendungen; **erst danach** Race-Algorithmus und vorläufige Wahl-/Konfliktnachrichten finalisieren. Keine exklusive Grant-/ABORT-Lösung ist vorgegeben. Danach Eindeutigkeit, Bindung, Crash-/Reconnect-Verhalten und Membership-Übergänge verifizieren.
- Exakte On-Disk-Record-/Index-/Checkpoint-Kodierung, Prüfsummen, Segmentgrößen, Sparse-Extent-Index, Block-COW-/Redo für Append-Arenen und Codecprofile (`raw/gzip/lz4/xz`) einschließlich Metadaten-/Speicheraufschlägen.
- Recovery unbestätigter Replica-Tails präzisieren: Reads bleiben ohne Primary und ohne Warten auf laufende Mutationen auf dem letzten lokal committed Stand möglich (§11.1.1); nur vorbereitete Versionen sind unsichtbar, verbindliche COMMITs werden nicht zurückgenommen. Unterschiedliche Replica-Sichten während eines laufenden Writes sind erlaubt. Ein Transportfehler allein beweist keinen Prozessausfall; ALL-Write-/Fencing-Regeln bleiben erhalten.
- Dedupe-Retention, explizite Clientfreigabe und Verhalten nach Ablauf, DELETE/Neuanlage/TRUNC. Request-IDs und ihr Zweck sind §10.3 erläutert. RAM hält Dedupe in lebenden Replikaten; nach Verlust aller Kopien gilt der Runtime-Reset-Vertrag.

### 20.2 API-Randfälle und Ressourcen

- GET_RANGE genau am/hinter EOF: leerer Erfolg oder 416? Null-Längen und Integerüberläufe; TRUNC bei fehlendem Objekt. PUT_RANGE-Neuanlage, Nullfüllung und TRUNC-Vergrößerung sind entschieden.
- Queue-/Staging-/In-flight-Budgets, Stream-Cancellation und konkrete Kontingentgrößen; verteilte logische Credits und physische Reserven müssen beide gelten. Metadaten-/Codecaufschläge für Quota-Konfiguration konservativ bestimmen.
- Exakte Hash-Domain-/Key-Längenkodierung und Golden Vectors Go/JS/C/Rust. Native 256-Bit-IDs gelten definitionsgemäß als eindeutig.

### 20.3 Protokoll und Betrieb

- Maschinelles Codec-Schema/Golden Vectors für die entschiedenen Nachrichten; Race-bezogene Einträge erst nach Messphase festschreiben. Physischer On-Disk-Codec bleibt unabhängig vom logischen CopyRecord-Transport.
- Konkrete System-Bucket-Objektschlüssel/-Schemas für Account-/ACL-Verifier, System-Volume-Startgröße, Credential-Rotation und Browser-Session-Recovery; keine Zertifikats-/CA-Einrichtung.
- Rolling Upgrades, Capability-Negotiation und Formatmigration, insbesondere für Kompressions-/Layoutwechsel; Features nur bei Unterstützung aller betroffenen Peers aktivieren.

### 20.4 Frontends und spätere Features

- Multi-Mount-FUSE: CAS-/Intent-Protokolle für Rename/Link/Unlink über mehrere Metadatenobjekte und Recovery. Alte Testcaches sind bis SUBSCRIBE erlaubt; bei dessen Einführung müssen Cache-/Kernel-Sichtbarkeit konsistent umgesetzt werden.
- S3: Objektformate für Key-/Prefix-Index, Pagination und Multipart-Uploadzustände/Complete/Abort. LIST und Multipart gehören zum ersten Frontend-Release; ihr Umfang wird nicht erneut zur Disposition gestellt. Blockdevice: ABI, Flush-/Trim-/Resize-Vertrag und QEMU-Adapter.
- SUBSCRIBE/UNSUBSCRIBE: Journal-Retention, Resync für langsame Leser, Relay-/Reconnect-Handoff und Ende durch Rechteentzug. Snapshot-/Versionsbarriere und Übergabe bei Migration stehen bereits fest.
- CACHE-Strategie: Verdrängungs-/Miss-Verhalten und Klassenwahl; bis dahin reserviert und Aktivierung mit 501 ablehnen. RAM-Replikation und Replica-Restore gehören zu v1.
- ENet: Channel-Pool, optionale cgo-Builds, HTTP-Authentifizierung zur Sessionbindung, Transportversion und Ordered-Stream-Reconnect-Barrieren; RELIABLE_UNORDERED ist kein nativer ENet-Modus (§13.9). Keine TLS-/Zertifikatsabhängigkeit hinzufügen.
- EC und mutable Read-Cache-Kohärenz: Stripe-/Segmentlayout, Append-Vorstufe und Fragmentrecovery. Zusätzliche READ_CACHEs bleiben von normalen gültigen RAM-/Disk-Replikaten getrennt.

### 20.5 Noch abzustimmende Vertragsfragen

1. **Sichere Bindung der Online-Teilnehmer:** Der Rebuild darf Offline-Placement nicht verändern und benötigt nur die in §4.3 gebundenen Online-Teilnehmer. Noch zu entwerfen und zu verifizieren sind deren eindeutige Bindung bei asymmetrischer Erreichbarkeit/Netzwerkpartitionen, der Ausschluss konkurrierender aktivierter Generationen und die geordnete Nachführung ausgelassener Nodes. Bloße Erreichbarkeit beweist weder eine sichere Teilnehmermenge noch das Ende einer bereits gebundenen Runde. Globale Membership-/Admin-Commits behalten die ALL-Member-Regel aus §4.5.
2. **Dedupe-Freigabe:** Sollen Clients abgeschlossene Operationen ausdrücklich freigeben oder gilt eine dokumentierte Retention? Wie wird ein Retry nach Ablauf beantwortet, ohne still erneut zu mutieren?
3. **Verbleibende Objektrandfälle:** GET_RANGE am/hinter EOF und TRUNC eines fehlenden Objekts; die bereits entschieden Range-/Null-/Vergrößerungsregeln bleiben unverändert.

---

## 21. Alleinstellungsmerkmale und Produktpositionierung

STQR ist ein **Single-Node-First, Range-Partitioned Distributed Object Store** für Binärobjekte unterschiedlicher Größe mit direktem Client-Routing. Es soll keine Datenbank und kein vollständiger POSIX-Dateisystemkern sein. Die nachfolgenden Alleinstellungsmerkmale sind **angestrebte Kombinationen von Fähigkeiten**, keine Behauptung, dass jeder Baustein allein neu oder unübertroffen wäre.

### 21.1 Differenzierungsmerkmale

1. **Vom lokalen Volume zum Cluster ohne API-Wechsel:** Ein einzelner Node mit einer regulären Volume-Datei ist vollständig betriebsfähig. Weitere gleichberechtigte Nodes treten über Cluster-Token und Peeradresse bei; Platzierung und Redundanz können anschließend generationsweise ausgebaut werden. Die Verfügbarkeit bei Ausfällen richtet sich weiterhin nach tatsächlich vorhandenen Replikaten und Quorum.
2. **Explizite, extrem kompakte Hash-Range-Map:** Der 256-Bit-Hash dient nur der stabilen logischen Adressierung. Die tatsächliche Position ist durch veränderbare, sortierte Range-Starts und Locations festgelegt. Neue Hardware erzwingt nicht automatisch eine CRUSH-artige Neuzuordnung aller betroffenen Ranges.
3. **Split/Merge unabhängig vom Kopieren:** Eine Range kann zunächst rein logisch halbiert oder mit einer kompatiblen Nachbarrange vereinigt werden; erst ein davon unabhängiger Placement-Entscheid verursacht Datenbewegungen. Das erlaubt kontrolliertes, priorisiertes und möglichst spätes Rebalancing.
4. **Eine Placement-Abstraktion für mehrere Ziele:** Tatsächliche Bytes, Read-/Write-/Append-IOPS, Auslastung, Volume-Klasse, Redundanz und Hotspot-Isolation fließen in dieselbe Planner-Schnittstelle ein. Ein getrennter Request-Router kann Reads zusätzlich auf gültige Replikate/Caches verteilen.
5. **Administrativ definierte Bucket-SLAs:** Pro Bucket lassen sich HDD-/SSD-/RAM-Basisspeicher, Replikations-/EC-Strategien, Cache-Berechtigung, Quotas und Benutzerzugriff festlegen. Änderungen werden als **NEXT-Placement-Generation** umgesetzt; `desired` und `effective` dürfen nicht verwechselt werden.
6. **Zielgerichteter kleiner Objektkern:** Ein eigenes, append-orientiertes Volumenformat mit Hash-Index und Segmentspeicherung soll für viele 2–250-KiB-Beispielobjekte weniger Dateisystem- und Metadatenoverhead erzeugen als eine Datei pro Objekt. Die tatsächlichen Vorteile sind zu benchmarken.
7. **Durable atomisches APPEND als native Storage-Operation:** Pro Objekt serialisiertes Append, stabiler Offset, Flush vor ACK sowie persistente Retry-Deduplizierung sind Kernfunktionen für Journale und Logs; dafür braucht STQR keine verteilte Multi-Object-Transaktionsengine.
8. **Direkt nutzbare allgemeine Clients:** Ein natives Binärprotokoll und Go-/C-/Rust-SDKs dienen beliebigen Anwendungen. FUSE, lokales S3-Gateway und NBD/Blockdevice werden als eigenständige Frontends angebunden.
9. **Ein Transport, rollenbasierte Fähigkeiten:** Dasselbe kompakte Protokoll wird von Clients, Nodes und Administratoren verwendet; die initiale Authentifizierung und die vergebene Rolle begrenzen jeweils die erlaubten Operationen.

### 21.2 Was ausdrücklich kein Alleinstellungsmerkmal ist

Range-Splitting, explizite Placement-Tabellen, auf Messwerten basierende Verteilung, Replica-Caching oder append-orientierte Object Stores existieren bereits in anderen Projekten. ScyllaDB-Tablets, FoundationDB-Data-Distribution, Ceph-PG-Upmap und SeaweedFS-Volumes sind wichtige Referenzen. **STQR differenziert sich durch sein zugeschnittenes Gesamtprofil**, nicht durch einen Anspruch auf Erfindung dieser Einzelverfahren. Eine behauptete Überlegenheit gegenüber diesen Systemen ist vor belastbaren Tests unzulässig.

---

## 22. Stärken und erwartete Betriebsqualitäten

| Stärke | Ursache im Design | Messbares Abnahmekriterium |
|---|---|---|
| Geringe Einstiegshürde | Single Node, Datei-Volume, identische APIs | Funktionierender Start und Crash-Recovery ohne drei Server |
| Präzises Kapazitätsbalancing | Größenstatistiken pro teilbarer Range | Maximaler Volume-/Node-Füllstand, Migrationsbytes, verbleibender Headroom |
| Planbare Datenmigration | CURRENT/NEXT, Hintergrundkopie, Catch-up, Cutover | P99-Clientlatenz während Rebalance, Durchsatz, Unterbrechungszeit |
| Kleine Client-Metadaten | Sortierte Range-Starts, segmentierte adaptive Client-Maps und Map-Pushes | Map-Größe je PB / je Bucket und Lookup-P99 |
| Keine zentrale Datenweiterleitung | Client routet direkt anhand der Map | Zusätzliche Hops und Bytes je `GET`/`PUT` |
| Flexible Storage- und Kostenstufen | Bucket-Policies und HDD/SSD/RAMCACHE-Klassen | Erfüllung von Tier-/Quota-Vorgaben, Kosten/GB, Cache-Hit-Rate |
| Sehr kleine Objekte effizient verwalten | Segmentbasiertes Format und lokaler Hash-Index | CPU/op, Write-Amplification, physische Bytes/Nutzbyte |
| Zuverlässige Logs | Atomisches, idempotent wiederholbares APPEND | Crash-/Retry-Test ohne Doppel-Append, Verlust oder Offset-Überlappung |
| Ausbaubare Zugriffspfade | Native SDKs und getrennte Gateways | End-to-End-Interoperabilität über Go/C/Rust, S3, FUSE, NBD |

**Einordnung:** Diese Stärken sind Designziele und teilweise strukturell plausibel. Ihre quantitative Wirkung muss unter identischer Hardware, Durability-Policy, Replikationsstufe, Objektgrößenverteilung und Last mit Alternativen verglichen werden.

---

## 23. Schwächen, Grenzen, Risiken und Vergleich mit Alternativen

STQR reduziert einige spezifische Zielkonflikte anderer Systeme, **beseitigt aber nicht die fundamentalen Kosten verteilter Speicherung**. In einem frühen Projektstadium dürften vor allem Betriebsreife, Testabdeckung und Ökosystem deutlich schwächer sein als bei Ceph.

### 23.1 Konkrete Trade-offs

| Aspekt | Möglicher STQR-Vorteil | Verbleibende Schwäche / Risiko | Gegenmaßnahme |
|---|---|---|---|
| Gegenüber Ceph | Kleinere, größenbewusste Migrationseinheiten; Single-Node-First | Ceph hat ausgereifte Recovery-, Scrubbing-, Security- und Betriebswerkzeuge sowie RBD/CephFS-Integration | Differenzierte Benchmarks, Recovery-Tests, eigene Frontend-Integration |
| Gegenüber ScyllaDB/FoundationDB | Kein darunterliegender Datenbankstack für reine Objekt-Payloads nötig | Deren verteilte Koordination, Split/Merge und Crash-Tests sind viel länger erprobt | Protokolle und Teststrategien studieren, nicht ungeprüft nachbauen |
| Gegenüber SeaweedFS | Explizite adaptive Hash-Ranges und gemeinsame Placement-Policies | SeaweedFS besitzt reife Blob-/Volume-Engine und Frontends | Metadaten- und Small-Object-Overhead messen |
| Gegenüber lokalem Dateisystem | Clusterweite Platzierung und Redundanz | Netzwerk, Auth, Index, Replikation, Konsens und Compaction erzeugen zusätzliche Kosten | Native Single-Node-Fast-Paths, Group Commit, Profiling |
| Globale Range-Map | Direktes Routing ohne zentralen Lookup | Sehr viele Buckets, Hot-Range-Splits und Replica-Locations können Maps aufblähen | Kompression, Teilmaps, Versionierung, Map-Größenlimits |
| Lazy Migration | Geringere Lastspitzen | Quell-/Zielversionen, Deletes, Tombstones und abgebrochene Moves sind komplex | Eindeutige Write-Authority, Fencing, persistenter Cutover |
| Replikation | Hohe Availability bei ausreichenden Failure Domains | Zusätzliche Schreibbytes, Netzwerktraffic und ACK-Latenz | Klar getrennte Durability-Policies, Placement nach Failure Domain |
| Erasure Coding | Bessere Kapazitätseffizienz bei geeignetem Workload | Kleine Objekte, Update/Append und Recovery können teuer sein | Zunächst Replikation, EC später segment-/stripebasiert evaluieren |
| RAM-Read-Replicas | Entlastung leselastiger Hotspots | Kohärenz bei mutablen Objekten; keinerlei Durability durch RAMCACHE | Versionierte Caches, Invalidierung/Leases, immutable Cache zuerst |
| Eigene Volume-Engine | Potenziell kleinerer Overhead pro Objekt | GC, Fragmentierung, stille Korruption, Index-Recovery und Wear sind neu zu lösen | Checksums, Scrub, Fault Injection, Format-Versionierung |
| Gemeinsames HTTP-/WebSocket-Protokoll | Weniger parallele Protokollstacks | Auth-Fehler können sowohl Control als auch Data Plane gefährden | Rollenbindung, Intranet-Zugriff, Rate Limits, getrennte Rechte und Tests |
| Go-Implementierung | Hohe Entwicklungs- und Wartbarkeit | Garbage Collection, Buffer-Kopien und `mmap`-/Zero-Copy-Integration können P99 beeinflussen | Pooling, Streaming, CPU-/Heap-Profile, isolierte Fast Paths |

### 23.2 Physikalische und konsistenzbedingte Grenzen

- **99 % Füllstand:** Kleine Ranges ermöglichen präzisere Moves, garantieren aber weder Platz auf dem Ziel noch ausreichend Platz für weitere Writes, Replikataufbau oder einen Host-Ausfall. Sicherheitsreserve und `full`-/`nearfull`-Regeln bleiben notwendig.
- **Zwei Generationen sind keine Transaktion:** Für garantierte linearisierbare Objektoperationen braucht es weiterhin Fencing, persistente Entscheidung über Ownership, Änderungsübernahme, Idempotenz und Crash-Recovery.
- **ALL-Barrieren und Warten:** Ein einzelner persistenter Node ist nicht host-ausfallsicher. Bereits vor einer Rebuild-Runde fehlende Nodes blockieren unabhängiges Online-Rebalancing nicht; ihr Placement bleibt eingefroren. Fehlende erforderliche Replikate blockieren betroffene Writes, fehlende Mitglieder globale Membership-/Admin-Entscheidungen und nach Bindung ausfallende Rundenteilnehmer den laufenden Rebuild. Warten bis Reconnect oder administrativem Forceful Disconnect bleibt in diesen Fällen eine bewusste Verfügbarkeitsgrenze.
- **Hot Objects:** Splitting isoliert ein heißes Objekt, verteilt aber dessen Writes nicht. Read-Cache-Skalierung funktioniert nur mit einer korrekten Versions-/Invalidierungsstrategie.
- **S3/POSIX/Block:** `LIST` braucht einen Namensindex; FUSE braucht Dateisystemmetadaten und Semantik; NBD braucht Flush/Discard/partielle Writes. Diese Gateways sind nicht bloß ein Protokoll-Wrapper.
- **Crash-Durability:** `ACK` darf nur nach persistenter Daten- und Recovery-Information erfolgen; SSD-/HDD-Controller müssen Flush/FUA korrekt unterstützen. Group Commit kann Flush-Aufwand zusammenfassen, aber nicht umgehen.

### 23.3 Vergleich – ohne unbewiesene Überlegenheit

| Kriterium | STQR-Zielbild | Ceph | ScyllaDB / FoundationDB | SeaweedFS |
|---|---|---|---|---|
| Primärzweck | Schlanker Object Store | Allgemeiner verteilter Storage | Verteilte Datenbanken | Datei-/Blob-/Object-Storage |
| Start mit einem Node | Designkern | Technisch möglich, HA typischerweise ≥3 Hosts | Je nach Modus möglich, produktive Topologie anders | Möglich |
| Explizit adaptive Ranges | Ja | PGs plus Upmap, anderer Granularitätsansatz | Ja, Tablets/Key-Ranges | Volumes statt Hash-Ranges |
| Platzierung nach echten Bytes | Geplant | Teilweise via externe/ergänzende Mechanismen | Vorhanden | Volumenbasierte Mechanismen |
| Clientseitig direktes Routing | Geplant | Ja, native Ceph-Clients | Teils / abhängig vom Request-Pfad | Ja, je nach API-Pfad |
| Kleine Objekte | Beispiel-Workload 2–250 KiB, kein Größenlimit | Allgemeiner Object Store | Datenbankorientiert | Blob-orientierte Engine |
| Native VM-/Dateisystem-Integration | NBD/FUSE geplant | Reif: RBD/CephFS | Nicht primärer Zweck | Verschiedene Frontends |
| Produktionsreife | Erst zu entwickeln | Hoch | Hoch | Etabliert |

Diese Tabelle bewertet **Ausrichtung und Architektur**, nicht aktuelle Benchmark-Sieger oder zugesicherte Eigenschaften einer noch nicht implementierten Version.

---

## 24. Einsatzzwecke und Eignung

### 24.1 Primäre Zielgruppen

1. **Datenbanken und Suchsysteme mit ausgelagerten Payloads:** Kleine bis mittelgroße unveränderliche oder überschreibbare Binärwerte; die Anwendung verwaltet eigene fachliche Metadaten und Transaktionen.
2. **Transaktionslogs, Journale und Event-Persistenz:** `APPEND` mit garantiertem Durable-ACK, monotoner Position und idempotenten Retries. STQR stellt keine eigene Stream-Processing-Engine oder Consumer-Offsets bereit.
3. **Object Storage für Anwendungssysteme und SaaS:** Mandanten- beziehungsweise Bucket-Trennung, individuelle Speicherklasse, Quotas, Rechte und S3-Zugriff über lokale Gateways.
4. **Skalierender Storage für kleine Deployments:** Anfang mit einem Server oder einer lokalen Datei; später Clusterbeitritt, zusätzliche Volumes und generationsweises Rebalancing ohne API-Wechsel.
5. **Hotspot-lastige Read-Workloads:** Häufig abgefragte Binärobjekte, Vorberechnungen, Artefakte oder Mediensegmente; Hot-Range-Isolation und zusätzliche gültige Read-Caches sind besonders nützlich.

### 24.2 Weitere vorgesehene Einsätze

| Einsatz | Frontend | Eignung / Bedingung |
|---|---|---|
| Online-Spiele, flüchtiger Shared State | Native RAM-Buckets | Replikation schützt bei Einzelprozess-Neustart; Verlust aller RAM-Prozesse leert die Daten |
| Anwendungscaches mit Laufzeitbindung | Native RAM-Buckets, später CACHE | RAM verdrängt nicht; CACHE darf später jederzeit evicten |
| Backup-Artefakte und kleine Blob-Archive | Native SDK / S3 | Gut bei vielen mittelgroßen Objekten; EC für kalte Daten erst nach Implementierung und Benchmarks |
| CI-Artefakte und Build-Caches | Native SDK / S3 | Gute Kandidaten für immutable Objekte und Read-Cache-Verteilung |
| Dateizugriff auf Object Storage | FUSE | Zunächst begrenzte POSIX-Semantik; keine unbelegte Mehrhost-POSIX-Garantie |
| VM-Images und allgemeine Blockgeräte | Blockdevice-Library, NBD, später direkter QEMU-Treiber | Nur mit korrekter Chunk-Atomizität, Flush/FUA, Trim, Snapshots nach Bedarf |
| HPC-/Analyse-Payloads | Native SDK | Geeignet, wenn Zugriffsmuster objektorientiert und nicht primär POSIX-lastig sind |
| Kalter kapazitätsorientierter Storage | HDD / später EC | Effizienzpotenzial, aber Recovery-Dauer und Platzreserve sorgfältig planen |
| Lesebeschleunigung als bezahltes Service-Tier | RAMCACHE-Read-Replicas | Nur mit kohärenter Read-Semantik; RAMCACHE zählt niemals als durable Replica |

### 24.3 Vorerst ungeeignete oder nachrangige Einsätze

- Vollständiger Ersatz für CephFS mit verteiltem POSIX-Locking und gleicher Semantik ab v1.
- Anwendungen, die automatischen Failover nach einem Failure-Timeout oder Verfügbarkeit trotz fehlender erforderlicher ALL-Mitglieder erwarten.
- Schreibintensive Hotspots auf **einem** einzelnen mutable Objekt, die sich ohne anwendungsseitiges Sharding parallelisieren sollen.
- Systeme, die allgemeine ACID-Transaktionen über mehrere Objekte erwarten.
- Unbegrenztes S3-Feature-Set oder sofortige vollständige POSIX-Kompatibilität.
- Einsatz unter kritischer produktiver Last vor überprüfter Crash-, Recovery-, Security- und Upgrade-Reife.

### 24.4 Benchmarks zur Bestätigung des Produktnutzens

STQR SOLL reproduzierbar gegen Ceph und eine geeignete lokale beziehungsweise volumenbasierte Object-Store-Baseline getestet werden. Die Tests müssen mindestens 2/4/16/64/200/250-KiB- und 1-MiB-Objekte sowie größere native Objekte, Read/Write/Append/CAS, synchronisierte persistente ACKs, 1×/2×/3×-Replikation, hohe physische Füllstände, Node-Ausfall mit Warten/Reconnect, administrativen Disconnect, Rebalance und Hotspot-Reads abdecken. RAM wird separat mit seinem flüchtigen Replica-/Restore-Vertrag gemessen. Zu protokollieren sind IOPS, Durchsatz, P50/P95/P99, CPU/op, RAM/Objekt, physische Bytes/Nutzbyte, Recovery-Zeit und Bytes pro Migrationsentscheidung. **Erst anhand dieser Werte dürfen quantitative Stärke- oder Kostenversprechen gemacht werden.**

---

## 25. Besondere Architekturentscheidungen – Kurzfassung

| Thema | STQR-Entscheidung |
|---|---|
| Sprache | Go |
| Start | Daemon startet/erzeugt Config und System-Bucket automatisch; keine init-/Format-CLI |
| Transport/Auth | Intranet HTTP/ws ohne TLS; Basic Auth, initiale Klartext-Credentials in Config |
| System-Bucket | persistent EVERYWHERE; Account-/Admin-Verifier und ACL als normale Objekte |
| Platzierungsmodus | count oder everywhere: vollständiger Bucket auf jeder aktiven Mitgliedsnode |
| Kompression | raw/gzip/lz4/xz pro Bucket und Objektversion; Migration konvertiert ins Zielverfahren |
| Sparse | ZERO-Extents für große Null-Lücken, logische Bytes ohne entsprechende Datenallokation |
| Nodes | gleichberechtigt, Cluster-Token + bekannte HTTP-Peers bzw. --connect |
| Generationen | maximal CURRENT/NEXT im Bulk-Balancing; nur Online-Teilnehmer umverteilen, Offline-Placement einfrieren; zurückgestellte Gründe nach Wiederkehr in neuer Generation umsetzen |
| Koordinator | Push-Wahl/Bindung als Ziel; konkrete Race-Behandlung erst nach Stresstestmessung entwerfen |
| Objektaddressierung | `SHA-256(bucket-id, object-key)` |
| Range-Spezifikation | inklusiver Start; nächster Start ist implizite Grenze |
| Split/Merge | binär splitten, kompatible Nachbarranges zusammenführen, möglichst ohne Kopieren |
| Balancer | wirkliche Bytes sowie Read-/Write-/Append-Last pro Range |
| Placement | flache mehrfach gleiche `from`-Werte gruppieren Locations; normierte Map für Sicherheit |
| Volumes | Datei/Blockdevice HDD/SSD, separater autoritativer RAM-Pool und RAMCACHE nur Read |
| Bucket-Strategien | HDD (SSD-Fallback), SSD und replizierbares RAM in v1; CACHE mit beliebiger Eviction für später reserviert |
| Persistenz | atomare Einzelobjektoperationen inkl. ranged PUT, TRUNC und CAS; ACK gemäß Disk-/RAM-Vertrag auf dem wirksamen Placement |
| Objektgröße | intern kein festes Limit; kleine Payloads sind Beispiel-Workloads |
| Frontend-Chunks | FUSE und Blockdevice-Library: 1 MiB pro Datenobjekt, kleinere ranged Transfers |
| Quotas | Guaranteed-Kapazität reserviert, Absolute als harte Grenze oder unlimited; Ablehnung mit 507 |
| Antworten | HTTP-Statuszahlen als uvarint, variabler Folgeheader; 301/302 mit Routing-Ziel |
| APPEND | linearisiert pro Objekt, Offset/Request-ID-Dedupe gemäß Disk-/RAM-Vertrag; Extent-Arenen für kleine Appends |
| Replica/EC | Bucket-Policy, konkrete Locations in Range-Map |
| Bucket-Storage-Policy-Change | Desired-Storage-Policy wird in **NEXT-Generation** umgesetzt; ACL und Quota folgen separatem Kontrollcommit |
| Zugriff | Admin verwaltet Buckets; User bekommt Objektzugriff pro Bucket; Frontend-Metadaten sind normale Objekte |
| Protokoll | HTTP/Basic, WebSocket-Upgrade, eine Binärmessage je Message; Q/F/P, gemeinsame Admin-/Peer-/Objektlayouts |
| Clients | native Go/C/Rust, Multi-Mount-FUSE mit CAS, S3 inkl. LIST/Multipart und Blockdevice-Lib |
| FUSE-Testcaches | stale bis SUBSCRIBE erlaubt, mit SUBSCRIBE konsistent |
| Replica-Reads | bei fehlendem Primary erlaubt; kein automatischer Schreib-Ownerwechsel |
| Prozessmodell | ein Daemon mit Dashboard/API, maximal volume/server/client als Library-Packages |
| Ausfall/Shutdown | kein automatischer Ausschluss; DRAIN, SHORT, CLUSTER und administrativer FORCE_DISCONNECT |
| Späterer Transport | Optional ENet; RELIABLE_ORDERED, RELIABLE_UNORDERED, UNRELIABLE_LATEST; gleicher STQR-Codec |
| Spätere Objektstreams | SUBSCRIBE/UNSUBSCRIBE samt geordneten ranged Updates und Migrations-Handoff |
| Dashboard | Baumtabelle Nodes→Volumes, Echtzeit Read-/Write-/Storage-Ticker, Bucket-Admin |
| Teststrategie | alle Zustandsübergänge, Crash-/Stresstests mit MiB-Volumes |

---

## 26. Schlussbild

STQR verbindet eine **sehr kleine, veröffentlichte und versionierte Range-Placement-Map** mit einem schlanken lokalen Object-Store-Format. Buckets definieren isolierte Storage-Policies und Rechte. Replikation, EC und zusätzliche Read-Caches werden durch **mehrere Locations je Range-Start** ausgedrückt; der Planner verschiebt Ranges, statt die Welt durch einen reinen Hash-Neuverteilungsalgorithmus automatisch neu zu platzieren.

Die drei wichtigsten technischen Verträge sind:

1. **Durable Object/APPEND:** Für HDD/SSD bedeutet `SUCCESS` synchronisierte, wiederauffindbare Bytes; RAM/CACHE folgen ihrem ausdrücklich flüchtigen Vertrag; `APPEND` ist pro Objekt atomar, geordnet und bei Retries dedupliziert.
2. **Single Ownership:** Kein Generationenwechsel erzeugt zwei gleichzeitig schreibberechtigte Primaries; veraltete Client-Maps sind sicher behandelbar.
3. **Separierte Policy und Placement:** Bucket-Konfiguration wird über die nächste Placement-Generation umgesetzt, ohne dass Clients eine andere Objekt-ID oder ein anderes API verwenden müssen.

Diese Verträge haben Vorrang vor einer einzelnen Performanceoptimierung. Damit ist STQR als **Single-Node-First, Range-Partitioned Distributed Object Store** vollständig genug umrissen, um die Detailprotokolle und einen implementierbaren Go-Prototyp daraus abzuleiten.

Die Bucket-Kontingentwahl verwendet dieselben dekadisch gestuften, bidirektionalen Größenregler wie Volumes und einen „Maximum“-Knopf. Grundlage ist freier nutzbarer Platz der passenden Datenvolumes über die verfügbaren Node-Kapazitätsbilder, abzüglich ungenutzter Garantien anderer Buckets; der Admin-Speicher bleibt ausgeschlossen. Beim absoluten Kontingent eines vorhandenen Buckets kommt seine bereits genutzte logische Größe hinzu. Die Umrechnung in planbare logische Bytes berücksichtigt das gewählte Profil „Typische Objektgröße“, darunter „1M (Blockdevices)“. Ein Prozentfeld wird nicht angezeigt. Der aktuelle Collector liefert die einzelne lokale Node; Peer-Aggregation verwendet später dieselben Kapazitätsbilder ohne erfundene Remote-Werte.
