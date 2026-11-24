# Debian-Pakete

Mit Go >= 1.24, git und dpkg-dev:

```sh
./tools/build-deb.sh 0.1.0
sudo apt install ./dist/stqr_0.1.0_amd64.deb
```

`DEB_ARCH=arm64` baut für ARM64, `GO` wählt den Go-Compiler. Das zweite
Argument wählt das Ausgabeverzeichnis. `STQR_RELEASE` ist eine über Releases
streng steigende Zahl (Standard: Commit-Zeitstempel); sie dient der Anzeige,
nicht der Freigabe von Requests. `--version` zeigt Paketversion und Git-Revision.

Der eigene `stqr.service` läuft als Benutzer `stqr` mit Arbeitsverzeichnis
`/var/lib/stqr`. Die Node erzeugt und pflegt `/etc/stqr/stqr.yaml`, einschließlich
des initialen Passworts. Volumes und Metadaten bleiben unter `/var/lib/stqr`,
solange in der Konfiguration keine anderen Pfade gesetzt sind. Blockdevices
müssen dem Dienstbenutzer gezielt zugänglich gemacht werden.

Vor dem Entpacken eines Updates stoppt das Paket den alten Dienst mit SIGTERM
und wartet auf dessen Ende. Das ist SHORT („warte auf mich“), kein Drain.
Nach erfolgreicher Installation startet der Dienst mit dem neuen Binary.
Dienststeuerung respektiert Debian `policy-rc.d`; deaktivierte Dienste bleiben
deaktiviert. Entfernen und Purge löschen keine Konfiguration oder Speicherdaten.

Eine bestehende PM2-Installation wird nicht automatisch übernommen: vor dem
Wechsel PM2-Prozess stoppen und dessen Autostart entfernen, anschließend die
vorhandenen Daten und Konfiguration mit passenden Besitzrechten übernehmen.
Zwei Daemons dürfen dieselben Volumes niemals gleichzeitig öffnen.

Rolling-Upgrades ändern keine Membership und lösen keinen Drain aus.
Versionsdetails erscheinen rot in der Node-Liste, wenn eine Node älter als eine
bekannte Version ist. Kompatible Requests bleiben möglich; inkompatible
Nachrichten werden vor ihrer Ausführung mit 426 abgelehnt. Ohne gemeinsames
Wire-Protokoll bleibt der Peer wie bei SHORT unerreichbar und wird weiterhin
kontaktiert. Er bleibt Owner seiner Daten und erforderlichen Replikate.

Der verbindliche Software Contract steht in `server/compatibility.go`.
Software vor Einführung dieses Contracts kann neue Formate nicht sicher
verarbeiten und muss vor deren Aktivierung angehalten beziehungsweise gefenced
werden. Das Paket aktiviert oder konvertiert selbst keine neuen Bucket-Formate.

## Discovery und Request-Semantik

Peer-WebSocket-Upgrades senden in beiden Richtungen `X-STQR-Compatibility`:
ein höchstens 8192 Byte großes JSON-Objekt mit `version`, `revision`, `release`,
`wire_min`, `wire_max` und optional `operations`. Dessen Schlüssel sind
vierstellige kleingeschriebene Hex-Opcodes; Werte enthalten `emit`, `min`, `max`
(jeweils u16). Fehlende Einträge bezeichnen ausschließlich die eingefrorenen
Nachrichten des v1-Baselines mit Revision 1. Neue Opcodes müssen beide Seiten
explizit unterstützen. Der Header ist nach Cluster-Authentifizierung und
NODE_HELLO-Identitätsbindung eine Versionsaussage, keine zusätzliche Berechtigung.

Ein Request ist zulässig, wenn beide Wire-Intervalle überlappen und beide Seiten
die gesendete Semantik des anderen akzeptieren. Die Prüfung gilt auch für
Weiterleitungen, innere Admin-Requests und nodeinterne Frontend-Verbindungen.
Sie erfolgt vor dem Zugriff auf die eigentlichen Nutzdaten. Ein fehlender Header
steht für das ursprüngliche v1-Profil. Die stabile Bootstrap-Schicht bleibt auch
bei vollständiger Inkompatibilität für die Diagnose benutzbar.

Das Dashboard abonniert Versionsmeldungen mit dem nativen Admin-Opcode `0x203a`:
Request-Body `Bool(enabled)`, Response `200 + Text(JSON(node_versions))`.
Nach `true` folgen Push-Nachrichten desselben Opcodes mit `Text(JSON(...))`;
`false` beendet sie. Normales STQR-Envelope, keine separate JSON-Verbindung.
Der Server pusht nur Änderungen, spätestens beim nächsten Statistikzyklus.
`NODE_CONFIG_GET` enthält dieselbe Ansicht bereits als `node_versions`.

Das aktuelle Profil behält die Nachrichtenhülle 1 bei. Öffentliche Objektclients
verwenden `stqr.v2`; `stqr.v1` bleibt nur für den unveränderten Peer-Bootstrap und
kompatible Diagnoseoperationen zugelassen. Unversionierte Objekt-Bodies,
Replikation und migrationsrelevante Kontrolldaten erfordern die passende
Opcode-Semantik. Release-Nummern allein erteilen keine Schreibberechtigung.
