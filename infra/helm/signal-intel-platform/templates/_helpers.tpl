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
Selector labels for a given component. Takes a dict: (dict "root" $ "component" "query-api").

Note: this used to take the component name directly (a bare string) and
reach for `$.Release.Name` inside the define -- but `include` executes the
named template with the passed argument as the new root, which reassigns
`$` to that argument for the duration of the call. So `$.Release.Name`
was evaluating `Release.Name` against the string "query-api" and failing
at render time (`helm template`/`helm install` would error on this template
for every service). Passing the outer `$` through explicitly, the same way
`signal-intel-platform.image` already does below, fixes it.
*/}}
{{- define "signal-intel-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ .component }}
app.kubernetes.io/instance: {{ .root.Release.Name }}
{{- end }}

{{/*
Resolves the fully qualified image reference for a component.
*/}}
{{- define "signal-intel-platform.image" -}}
{{- $registry := .root.Values.image.registry -}}
{{- printf "%s/%s:%s" $registry .repository .tag -}}
{{- end }}
