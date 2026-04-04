{{/*
Render an HTTP probe configuration block.
Usage: {{ include "kubernetes-event-logger.httpProbe" (dict "config" .Values.livenessProbe "port" .Values.port.containerPort "path" "/healthz") | nindent 10 }}
*/}}
{{- define "kubernetes-event-logger.httpProbe" -}}
httpGet:
  path: {{ .path }}
  port: {{ .port }}
  scheme: HTTP
initialDelaySeconds: {{ .config.initialDelaySeconds }}
periodSeconds: {{ .config.periodSeconds }}
timeoutSeconds: {{ .config.timeoutSeconds }}
failureThreshold: {{ .config.failureThreshold }}
{{- end }}

{{/*
Render a TCP probe configuration block (deprecated, use httpProbe instead).
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
