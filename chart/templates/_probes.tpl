{{/*
Render an HTTP probe configuration block.
Usage: {{ include "kubernetes-event-logger.httpProbe" (dict "config" .Values.livenessProbe "port" .Values.port.containerPort "path" "/healthz") | nindent 10 }}
*/}}
{{- define "kubernetes-event-logger.httpProbe" -}}
httpGet:
  path: {{ .path }}
  port: {{ .port }}
  scheme: HTTP
{{- if .config.initialDelaySeconds }}
initialDelaySeconds: {{ .config.initialDelaySeconds }}
{{- end }}
periodSeconds: {{ .config.periodSeconds }}
timeoutSeconds: {{ .config.timeoutSeconds }}
failureThreshold: {{ .config.failureThreshold }}
{{- end }}
