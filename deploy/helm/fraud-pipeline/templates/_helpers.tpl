{{- define "fraud-pipeline.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "fraud-pipeline.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s" (include "fraud-pipeline.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{- define "fraud-pipeline.labels" -}}
app.kubernetes.io/name: {{ include "fraud-pipeline.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{- define "fraud-pipeline.topicInit" -}}
until rpk cluster info -X brokers={{ .Values.kafka.bootstrap }} >/dev/null 2>&1; do
  echo "Waiting for Kafka..."
  sleep 2
done
until rpk topic describe {{ .Values.kafka.topic }} -X brokers={{ .Values.kafka.bootstrap }} >/dev/null 2>&1; do
  rpk topic create {{ .Values.kafka.topic }} --partitions {{ .Values.kafka.topicPartitions }} \
    --replicas {{ .Values.kafka.replicationFactor }} -X brokers={{ .Values.kafka.bootstrap }} || true
  sleep 2
done
CURRENT_PARTITIONS=$(rpk topic describe {{ .Values.kafka.topic }} --print-summary \
  -X brokers={{ .Values.kafka.bootstrap }} | awk '$1 == "PARTITIONS" { print $2 }')
if [ "$CURRENT_PARTITIONS" != "{{ .Values.kafka.topicPartitions }}" ]; then
  echo "Topic partition count differs; use a new topic/storage generation instead of remapping card keys."
  exit 1
fi
{{- end -}}
