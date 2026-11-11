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

{{/*
The Secret every pod reads passwords and keys from.
*/}}
{{- define "dataflow.secretName" -}}
{{- if .Values.externalSecrets.enabled }}
{{- .Values.externalSecrets.secretName }}
{{- else if .Values.secrets.create }}
{{- include "dataflow.fullname" . }}-secrets
{{- else }}
{{- required "secrets.existingSecret is required when externalSecrets.enabled and secrets.create are both false" .Values.secrets.existingSecret }}
{{- end }}
{{- end }}

{{/*
Environment shared by the API, worker and scheduler containers. The three
passwords come first, from the Secret, so the $(VAR) references in the
connection strings below always resolve.
*/}}
{{- define "dataflow.commonEnv" -}}
- name: REDIS_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "dataflow.secretName" . }}
      key: REDIS_PASSWORD
- name: POSTGRES_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "dataflow.secretName" . }}
      key: POSTGRES_PASSWORD
- name: MONGODB_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "dataflow.secretName" . }}
      key: MONGODB_PASSWORD
- name: DATAFLOW_ENVIRONMENT
  value: {{ .Values.global.environment | quote }}
{{- if or (eq .Values.global.environment "prod") (eq .Values.global.environment "production") }}
- name: DATAFLOW_ENV
  value: "production"
{{- end }}
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
- name: DATAFLOW_CONTROL_STORE_BACKEND
  value: {{ .Values.controlPlane.storeBackend | quote }}
- name: DATAFLOW_POD_LOCAL_STORES_ACK
  value: {{ .Values.controlPlane.podLocalStoresAck | quote }}
- name: DATAFLOW_CDC_LEASE_BACKEND
  value: "redis"
- name: DATAFLOW_REDIS_URL
  value: "redis://:$(REDIS_PASSWORD)@{{ .Values.data.redis.host }}:{{ .Values.data.redis.port }}/0"
- name: DATABASE_URL
  value: "postgresql://{{ .Values.data.rds.username }}:$(POSTGRES_PASSWORD)@{{ .Values.data.rds.host }}:{{ .Values.data.rds.port }}/{{ .Values.data.rds.database }}"
- name: MONGODB_URL
  value: "mongodb://{{ .Values.data.documentdb.username }}:$(MONGODB_PASSWORD)@{{ .Values.data.documentdb.host }}:{{ .Values.data.documentdb.port }}/{{ .Values.data.documentdb.database }}?replicaSet={{ .Values.data.documentdb.replicaSet }}&tls={{ .Values.data.documentdb.tls }}&retryWrites={{ .Values.data.documentdb.retryWrites }}{{ if .Values.data.documentdb.tlsCAFile }}&tlsCAFile={{ .Values.data.documentdb.tlsCAFile }}{{ end }}"
- name: DATAFLOW_S3_BUCKET
  value: {{ .Values.objectStore.s3.bucket | quote }}
- name: AWS_REGION
  value: {{ .Values.objectStore.s3.region | quote }}
# The root filesystem is read-only: everything the app writes goes to the
# data volume or /tmp (see dataflow.runtimeVolumes).
- name: DATAFLOW_DATA_DIR
  value: /data/data
- name: DATAFLOW_UPLOAD_DIR
  value: /data/uploads
- name: DATAFLOW_VECTOR_STORE_DIR
  value: /data/vector_store
- name: DATAFLOW_BLOB_ROOT
  value: /data/blobs
- name: HOME
  value: /tmp
- name: HF_HOME
  value: /data/cache/huggingface
- name: XDG_CACHE_HOME
  value: /data/cache
{{- end }}

{{- define "dataflow.secretEnvFrom" -}}
envFrom:
  - secretRef:
      name: {{ include "dataflow.secretName" . }}
{{- end }}

{{/*
Writable paths for a pod whose root filesystem is read-only.
*/}}
{{- define "dataflow.runtimeVolumeMounts" -}}
- name: data
  mountPath: /data
- name: tmp
  mountPath: /tmp
{{- with .Values.extraVolumeMounts }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{- define "dataflow.runtimeVolumes" -}}
- name: data
{{- if .Values.persistence.data.existingClaim }}
  persistentVolumeClaim:
    claimName: {{ .Values.persistence.data.existingClaim }}
{{- else }}
  emptyDir:
    sizeLimit: {{ .Values.persistence.data.sizeLimit }}
{{- end }}
- name: tmp
  emptyDir:
    sizeLimit: {{ .Values.persistence.tmp.sizeLimit }}
{{- with .Values.extraVolumes }}
{{ toYaml . }}
{{- end }}
{{- end }}

{{/*
Workers and the scheduler serve /health (public) and /metrics (authenticated)
on the metrics port; the probes use /health.
*/}}
{{- define "dataflow.metricsPortAndProbes" -}}
ports:
  - name: metrics
    containerPort: {{ .Values.metrics.port }}
    protocol: TCP
startupProbe:
  httpGet:
    path: /health
    port: metrics
  periodSeconds: 5
  failureThreshold: 36
livenessProbe:
  httpGet:
    path: /health
    port: metrics
  periodSeconds: 15
  timeoutSeconds: 5
  failureThreshold: 4
readinessProbe:
  httpGet:
    path: /health
    port: metrics
  periodSeconds: 10
  timeoutSeconds: 5
{{- end }}
