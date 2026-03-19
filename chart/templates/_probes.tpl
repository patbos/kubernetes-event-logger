{{/*
Render a TCP probe configuration block.
Usage: {{ include "kubernetes-event-logger.tcpProbe" (dict "config" .Values.livenessProbe "port" .Values.port.name) | nindent 10 }}
*/}}
{{- define "kubernetes-event-logger.tcpProbe" -}}
tcpSocket:
  port: {{ .port }}
initialDelaySeconds: {{ .config.initialDelaySeconds }}
periodSeconds: {{ .config.periodSeconds }}
timeoutSeconds: {{ .config.timeoutSeconds }}
failureThreshold: {{ .config.failureThreshold }}
{{- end }}
