{{/*
Chart name and version, used as a label suffix.
*/}}
{{- define "signal-intel-platform.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Fully qualified app name for a given component, e.g. "signal-intel-platform-query-api".
*/}}
{{- define "signal-intel-platform.componentName" -}}
{{- printf "%s-%s" .root.Release.Name .component | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels applied to every resource.
*/}}
{{- define "signal-intel-platform.labels" -}}
helm.sh/chart: {{ include "signal-intel-platform.chart" . }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: signal-intel-platform
{{- end }}

{{/*
Selector labels for a given component.
*/}}
{{- define "signal-intel-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ . }}
app.kubernetes.io/instance: {{ $.Release.Name }}
{{- end }}

{{/*
Resolves the fully qualified image reference for a component.
*/}}
{{- define "signal-intel-platform.image" -}}
{{- $registry := .root.Values.image.registry -}}
{{- printf "%s/%s:%s" $registry .repository .tag -}}
{{- end }}
