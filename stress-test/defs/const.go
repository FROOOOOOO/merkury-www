package defs

var (
	Urls      ActionMapper
	Resources ResourceMapper
	Endpoint  = "https://192.168.12.127:6443/"
	Reslist   = []string{"ns", "no", "pv",
		"cm", "ep", "limits", "pvc", "po", "podtemplate",
		"rc", "quota", "secret", "sa", "svc",
		"controllerrevision", "ds", "deploy", "rs", "sts",
		"cj", "job"}
)

const (
	Token = "" // the same as AUTH_TOKEN in core/utils/utils.py
)

func init() {
	Urls = make(ActionMapper)
	Urls["GET"] = make(GroupMapper)
	Urls["LIST (cluster)"] = make(GroupMapper)
	Urls["LIST (namespace)"] = make(GroupMapper)
	Urls["GET"]["core v1"] = []string{"api/v1/namespaces/default",
		"api/v1/nodes/test-master-0",
		"api/v1/persistentvolumes/task-pv-volume",
		"api/v1/namespaces/kube-system/pods/etcd-0-0",
		"api/v1/namespaces/kube-system/services/kube-dns",
		"api/v1/namespaces/kube-system/endpoints/kube-dns",
		"api/v1/namespaces/test/configmaps/kube-root-ca.crt",
		"api/v1/namespaces/test/limitranges/limit-range-demo",
		"api/v1/namespaces/test/podtemplates/pod-template-demo",
		"api/v1/namespaces/test/replicationcontrollers/rc-demo",
		"api/v1/namespaces/test/serviceaccounts/default",
		"api/v1/namespaces/test/resourcequotas/quota-demo",
		"api/v1/namespaces/kube-system/secrets/vpa-tls-certs",
		"api/v1/namespaces/test/events/myevent",
		"api/v1/componentstatuses/etcd-0",
		"api/v1/namespaces/test/persistentvolumeclaims/task-pv-claim"}
	Urls["GET"]["apps v1"] = []string{"apis/apps/v1/namespaces/kube-system/daemonsets/kube-proxy",
		"apis/apps/v1/namespaces/kube-system/controllerrevisions/kube-apiserver-1-644b779485",
		"apis/apps/v1/namespaces/kube-system/deployments/coredns",
		"apis/apps/v1/namespaces/kube-system/replicasets/coredns-65dcc469f7",
		"apis/apps/v1/namespaces/kube-system/statefulsets/etcd-0"}
	Urls["GET"]["batch v1"] = []string{"apis/batch/v1/namespaces/test/jobs/job-demo",
		"apis/batch/v1/namespaces/test/cronjobs/cronjob-demo"}
	Urls["LIST (cluster)"]["core v1"] = []string{"api/v1/namespaces",
		"api/v1/nodes",
		"api/v1/pods",
		"api/v1/services",
		"api/v1/endpoints",
		"api/v1/configmaps",
		"api/v1/limitranges",
		"api/v1/persistentvolumes",
		"api/v1/persistentvolumeclaims",
		"api/v1/podtemplates",
		"api/v1/replicationcontrollers",
		"api/v1/resourcequotas",
		"api/v1/secrets",
		"api/v1/events",
		"api/v1/serviceaccounts"}
	Urls["LIST (cluster)"]["apps v1"] = []string{"apis/apps/v1/daemonsets",
		"apis/apps/v1/controllerrevisions",
		"apis/apps/v1/deployments",
		"apis/apps/v1/replicasets",
		"apis/apps/v1/statefulsets"}
	Urls["LIST (cluster)"]["batch v1"] = []string{"apis/batch/v1/jobs",
		"apis/batch/v1/cronjobs"}
	Urls["LIST (namespace)"]["core v1"] = []string{
		"api/v1/namespaces/kube-system/pods",
		"api/v1/namespaces/default/services",
		"api/v1/namespaces/default/endpoints",
		"api/v1/namespaces/default/configmaps",
		"api/v1/namespaces/test/events",
		"api/v1/namespaces/test/limitranges",
		"api/v1/namespaces/test/persistentvolumeclaims",
		"api/v1/namespaces/test/podtemplates",
		"api/v1/namespaces/test/replicationcontrollers",
		"api/v1/namespaces/test/resourcequotas",
		"api/v1/namespaces/kube-system/secrets",
		"api/v1/namespaces/default/serviceaccounts"}
	Urls["LIST (namespace)"]["apps v1"] = []string{"apis/apps/v1/namespaces/kube-system/daemonsets",
		"apis/apps/v1/namespaces/kube-system/controllerrevisions",
		"apis/apps/v1/namespaces/kube-system/deployments",
		"apis/apps/v1/namespaces/kube-system/replicasets",
		"apis/apps/v1/namespaces/kube-system/statefulsets"}
	Urls["LIST (namespace)"]["batch v1"] = []string{"apis/batch/v1/namespaces/test/jobs",
		"apis/batch/v1/namespaces/test/cronjobs"}

	Resources = make(ResourceMapper)
	Resources["v1"] = ResourceStore{"Namespace": {}, "Node": {}, "PersistentVolume": {}}
	Resources["v1-n"] = ResourceStore{"ConfigMap": {}, "Endpoints": {}, "LimitRange": {}, "PersistentVolumeClaim": {}, "Pod": {}, "PodTemplate": {},
		"ReplicationController": {}, "ResourceQuota": {}, "Secret": {}, "ServiceAccount": {}, "Service": {}}
	Resources["apps/v1"] = ResourceStore{"ControllerRevision": {}, "DaemonSet": {}, "Deployment": {}, "ReplicaSet": {}, "StatefulSet": {}}
	Resources["batch/v1"] = ResourceStore{"CronJob": {}, "Job": {}}
}
