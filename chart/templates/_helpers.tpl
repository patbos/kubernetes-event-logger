{{/*
Expand the name of the chart.
*/}}
{{- define "kubernetes-event-logger.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "kubernetes-event-logger.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart label.
*/}}
{{- define "kubernetes-event-logger.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels.
*/}}
{{- define "kubernetes-event-logger.labels" -}}
helm.sh/chart: {{ include "kubernetes-event-logger.chart" . }}
{{ include "kubernetes-event-logger.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels.
*/}}
{{- define "kubernetes-event-logger.selectorLabels" -}}
app.kubernetes.io/name: {{ include "kubernetes-event-logger.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
ServiceAccount name.
*/}}
{{- define "kubernetes-event-logger.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "kubernetes-event-logger.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- required "serviceAccount.name must be set when serviceAccount.create=false" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Leader election Lease name.
*/}}
{{- define "kubernetes-event-logger.leaseName" -}}
{{- include "kubernetes-event-logger.fullname" . -}}
{{- end }}

{{/*
Build the container image reference.
If a digest is provided, prefer an immutable reference. Keep the tag when available
so the rendered image remains easy to read while the digest pins the actual content.
*/}}
{{- define "kubernetes-event-logger.image" -}}
{{- $repository := .Values.image.repository -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- $digest := .Values.image.digest -}}
{{- if $digest -}}
{{- if $tag -}}
{{- printf "%s:%s@%s" $repository $tag $digest -}}
{{- else -}}
{{- printf "%s@%s" $repository $digest -}}
{{- end -}}
{{- else -}}
{{- printf "%s:%s" $repository $tag -}}
{{- end -}}
{{- end }}

{{- define "kubernetes-event-logger.excludeFilter" -}}
{{- $parts := list -}}
{{- with .namespace -}}
{{- $parts = append $parts (printf "namespace=%s" .) -}}
{{- end -}}
{{- with .kind -}}
{{- $parts = append $parts (printf "kind=%s" .) -}}
{{- end -}}
{{- with .name -}}
{{- $parts = append $parts (printf "name=%s" .) -}}
{{- end -}}
{{- with .reason -}}
{{- $parts = append $parts (printf "reason=%s" .) -}}
{{- end -}}
{{- with .type -}}
{{- $parts = append $parts (printf "type=%s" .) -}}
{{- end -}}
{{- with .reportingComponent -}}
{{- $parts = append $parts (printf "reporting-component=%s" .) -}}
{{- end -}}
{{- with .sourceComponent -}}
{{- $parts = append $parts (printf "source-component=%s" .) -}}
{{- end -}}
{{- join "," $parts -}}
{{- end }}
