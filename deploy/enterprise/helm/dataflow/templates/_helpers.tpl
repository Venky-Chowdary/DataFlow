{{/*
Common helpers for the DataFlow Helm chart.
*/}}

{{- define "dataflow.name" -}}
dataflow
{{- end }}

{{- define "dataflow.fullname" -}}
{{ .Release.Name }}
{{- end }}

{{- define "dataflow.chart" -}}
{{ .Chart.Name }}-{{ .Chart.Version }}
{{- end }}

{{- define "dataflow.labels" -}}
helm.sh/chart: {{ include "dataflow.chart" . }}
{{ include "dataflow.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "dataflow.selectorLabels" -}}
app.kubernetes.io/name: {{ include "dataflow.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{- define "dataflow.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "dataflow.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{- define "dataflow.commonEnv" -}}
- name: DATAFLOW_ENVIRONMENT
  value: {{ .Values.global.environment | quote }}
- name: DATAFLOW_REQUIRE_AUTH
  value: {{ .Values.auth.requireAuth | quote }}
- name: DATAFLOW_ENABLE_DOCS
  value: {{ .Values.auth.enableDocs | quote }}
- name: DATAFLOW_TRAINING
  value: {{ .Values.auth.training | quote }}
- name: DATAFLOW_SEED_DEMO
  value: {{ .Values.auth.seedDemo | quote }}
- name: DATAFLOW_MULTI_REPLICA
  value: "1"
- name: DATAFLOW_JOB_STORE
  value: "mongodb"
- name: DATAFLOW_CDC_LEASE_BACKEND
  value: "redis"
- name: DATAFLOW_REDIS_URL
  value: "redis://:$(REDIS_PASSWORD)@{{ .Values.data.redis.host }}:{{ .Values.data.redis.port }}/0"
- name: DATABASE_URL
  value: "postgresql://{{ .Values.data.rds.username }}:$(POSTGRES_PASSWORD)@{{ .Values.data.rds.host }}:{{ .Values.data.rds.port }}/{{ .Values.data.rds.database }}"
- name: MONGODB_URL
  value: "mongodb://{{ .Values.data.documentdb.username }}:$(MONGODB_PASSWORD)@{{ .Values.data.documentdb.host }}:{{ .Values.data.documentdb.port }}/{{ .Values.data.documentdb.database }}?replicaSet=rs0&ssl=false"
- name: DATAFLOW_S3_BUCKET
  value: {{ .Values.objectStore.s3.bucket | quote }}
- name: AWS_REGION
  value: {{ .Values.objectStore.s3.region | quote }}
{{- end }}

{{- define "dataflow.secretEnvFrom" -}}
{{- if .Values.externalSecrets.enabled }}
envFrom:
  - secretRef:
      name: {{ .Values.externalSecrets.secretName }}
{{- end }}
{{- end }}
