# datamonitoring-slack
Dies ist das Begleit-Repository zum Artikel im Analytics Pioneers 


## Aufbau

### example_data_generator

Dieser Ordner enthält eine sql-Datei die manuell in BigQuery ausgeführt werden kann. Sie erstellt eine Tabelle und befüllt diese mit Beispieldaten um die Funktionsweise des Datamonitorings direkt ausprobieren zu können.

### prepare_data_query

Dieser Ordner enthält eine sql-Datei, die die Daten für die Darstellung in der eigentlichen Python-Funktion vorbereitet. Sie wird bei jeder Ausführung der Cloud Function getriggert.

Es ist unbedingt darauf zu achten, dass diese Query im Live-Betrieb nicht zuviele Datenvolumen abruft, sonst können hohe Kosten entstehen.

Analysten sollten gemäß der im Artikel beschriebenen Vorgehensweise ausschließlich in dieser Datei arbeiten. 


### main.py

Diese Datei enthält die eigentliche Cloud Function und sollte gemeinsam mit der requirements.txt und dem Ordner prepare_data_query deployed werden.

Damit die Funktionalität des Gitlab-Buttons hergestellt werden kann, muss noch der Link `<http://gitlab.com/organization/group/project/-/issues/new?issue[title]=Data%20{today}&issuable_template=Incident%20Data&issue[issue_type]=incident|create issue.>` angepasst werden. 
