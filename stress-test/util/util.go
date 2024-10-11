package util

import "stressTest/defs"

func Res2uri(res string) (uri string) {
	if res == "ns" || res == "namespace" {
		uri = "namespaces"
	} else if res == "no" || res == "node" {
		uri = "nodes"
	} else if res == "pv" || res == "persistentvolume" {
		uri = "persistentvolumes"
	} else if res == "po" || res == "pod" {
		uri = "pods"
	} else if res == "svc" || res == "service" {
		uri = "services"
	} else if res == "ep" || res == "endpoint" || res == "endpoints" {
		uri = "endpoints"
	} else if res == "cm" || res == "configmap" {
		uri = "configmaps"
	} else if res == "limits" || res == "limitrange" {
		uri = "limitranges"
	} else if res == "podtemplate" {
		uri = "podtemplates"
	} else if res == "rc" || res == "replicationcontroller" {
		uri = "replicationcontrollers"
	} else if res == "sa" || res == "serviceaccount" {
		uri = "serviceaccounts"
	} else if res == "quota" || res == "resourcequota" {
		uri = "resourcequotas"
	} else if res == "secret" {
		uri = "secrets"
	} else if res == "pvc" || res == "persistentvolumeclaim" {
		uri = "persistentvolumeclaims"
	} else if res == "ds" || res == "daemonset" {
		uri = "daemonsets"
	} else if res == "controllerrevision" {
		uri = "controllerrevisions"
	} else if res == "deploy" || res == "deployment" {
		uri = "deployments"
	} else if res == "rs" || res == "replicaset" {
		uri = "replicasets"
	} else if res == "sts" || res == "statefulset" {
		uri = "statefulsets"
	} else if res == "job" {
		uri = "jobs"
	} else if res == "cj" || res == "cronjob" {
		uri = "cronjobs"
	}
	return uri
}
func Res2kind(res string) string {
	kind := ""
	if res == "ns" || res == "namespace" {
		kind = "Namespace"
	} else if res == "no" || res == "node" {
		kind = "Node"
	} else if res == "pv" || res == "persistentvolume" {
		kind = "PersistentVolume"
	} else if res == "po" || res == "pod" {
		kind = "Pod"
	} else if res == "svc" || res == "service" {
		kind = "Service"
	} else if res == "ep" || res == "endpoint" || res == "endpoints" {
		kind = "Endpoints"
	} else if res == "cm" || res == "configmap" {
		kind = "ConfigMap"
	} else if res == "limits" || res == "limitrange" {
		kind = "LimitRange"
	} else if res == "podtemplate" {
		kind = "PodTemplate"
	} else if res == "rc" || res == "replicationcontroller" {
		kind = "ReplicationController"
	} else if res == "sa" || res == "serviceaccount" {
		kind = "ServiceAccount"
	} else if res == "quota" || res == "resourcequota" {
		kind = "ResourceQuota"
	} else if res == "secret" {
		kind = "Secret"
	} else if res == "pvc" || res == "persistentvolumeclaim" {
		kind = "PersistentVolumeClaim"
	} else if res == "ds" || res == "daemonset" {
		kind = "DaemonSet"
	} else if res == "controllerrevision" {
		kind = "ControllerRevision"
	} else if res == "deploy" || res == "deployment" {
		kind = "Deployment"
	} else if res == "rs" || res == "replicaset" {
		kind = "ReplicaSet"
	} else if res == "sts" || res == "statefulset" {
		kind = "StatefulSet"
	} else if res == "job" {
		kind = "Job"
	} else if res == "cj" || res == "cronjob" {
		kind = "CronJob"
	}
	return kind
}
func Kind2ns_and_version(kind string) (namespaced bool, api_version string) {
	if _, ok := defs.Resources["v1"][kind]; ok {
		api_version = "v1"
	} else if _, ok := defs.Resources["v1-n"][kind]; ok {
		namespaced = true
		api_version = "v1"
	} else if _, ok := defs.Resources["apps/v1"][kind]; ok {
		namespaced = true
		api_version = "apps/v1"
	} else {
		api_version = "batch/v1"
		namespaced = true
	}
	return namespaced, api_version
}
