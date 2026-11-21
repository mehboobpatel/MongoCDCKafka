{{/*
_helpers.tpl — mongo-stream-cdc
Shared template helpers.
*/}}

{{/* Chart labels */}}
{{- define "msc.labels" -}}
helm.sh/chart: {{ .Chart.Name }}-{{ .Chart.Version }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/* Full image reference */}}
{{- define "msc.image" -}}
{{ .Values.image.repository }}:{{ .Values.image.tag }}
{{- end }}

{{/* MONGO_URI env from secret */}}
{{- define "msc.mongoEnv" -}}
- name: MONGO_URI
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: MONGO_URI
{{- end }}

{{/* POSTGRES_URI env from secret */}}
{{- define "msc.postgresEnv" -}}
- name: POSTGRES_URI
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: POSTGRES_URI
{{- end }}

{{/* Kafka auth env from secret */}}
{{- define "msc.kafkaEnv" -}}
- name: KAFKA_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: KAFKA_USERNAME
- name: KAFKA_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ .Values.existingSecret }}
      key: KAFKA_PASSWORD
{{- end }}

{{/* Standard volume mounts (config + table-config) */}}
{{- define "msc.volumeMounts" -}}
- name: config-vol
  mountPath: /config
- name: table-config-vol
  mountPath: /table-config
{{- end }}

{{/* Standard volumes (config + table-config) */}}
{{- define "msc.volumes" -}}
- name: config-vol
  configMap:
    name: msc-config
- name: table-config-vol
  configMap:
    name: msc-table-config
{{- end }}

{{/* Standard args for all services */}}
{{- define "msc.args" -}}
- "--config"
- "/config/config.json"
- "--table-config"
- "/table-config/combined_table_config_3.json"
{{- end }}

