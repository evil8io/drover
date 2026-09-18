{{- define "rancher-namespace-filter.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rancher-namespace-filter.fullname" -}}
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

{{- define "rancher-namespace-filter.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "rancher-namespace-filter.version" -}}
{{- default .Chart.AppVersion .Values.image.tag }}
{{- end }}

{{- define "rancher-namespace-filter.selectorLabels" -}}
app.kubernetes.io/name: {{ include "rancher-namespace-filter.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "rancher-namespace-filter.labels" -}}
helm.sh/chart: {{ include "rancher-namespace-filter.chart" . }}
{{ include "rancher-namespace-filter.selectorLabels" . }}
app.kubernetes.io/version: {{ include "rancher-namespace-filter.version" . | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "rancher-namespace-filter.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "rancher-namespace-filter.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "rancher-namespace-filter.tokenSecretName" -}}
{{- if .Values.token.existingSecret }}
{{- .Values.token.existingSecret }}
{{- else }}
{{- printf "%s-token" (include "rancher-namespace-filter.fullname" .) }}
{{- end }}
{{- end }}
