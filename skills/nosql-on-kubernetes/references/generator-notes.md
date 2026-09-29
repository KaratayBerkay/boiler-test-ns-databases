# compose → Kubernetes: what `k8s/gen.py` does with each construct (ns-databases)

| compose | Kubernetes | note |
|---|---|---|
| long-running service | Deployment (replicas 1, strategy Recreate) + headless Service (`clusterIP: None`, `publishNotReadyAddresses: true`) | DNS name = service name; a second headless Service for `container_name` when it differs |
| `ports: "host:container"` | NodePort Service `<name>-np`, nodePort from `k8s/ports.json` (host port → 31000+) | k3d publishes `127.0.0.1:<nodePort>`; the harness translates lab.yaml ports |
| `x-k8s: {publish: {"9042": 19042}}` | as if compose had published 9042 on host port 19042 | for stacks reached by container IP (Cassandra) |
| `x-k8s: {port: N}` | the port other services' `wait-for` initContainers probe | when a service publishes nothing |
| `x-k8s: {env: {K: V}}` | env override for k8s only; `$${NODEPORT:hp}` → nodePort of host port hp | Neo4j advertised Bolt address, ES memlock, Cassandra seeds/JMX |
| named volume | PersistentVolumeClaim of the same name (RWO, local-path) | shared by every pod that mounts it |
| bind mount (file / dir) | ConfigMap (subPath for a file) | binary files skipped with a warning |
| `restart: "no"` one-shot | initContainer of its dependents when it only prepares volumes, else a Job with `wait-for` initContainers | Mongo `rs-init.js` becomes a Job that waits for mongo1-3 |
| `depends_on: service_healthy` | initContainer `wait-for-<dep>` (busybox `nc -z`) | Jobs resolve to what they waited for |
| `healthcheck` | readinessProbe + startupProbe (`start_period`/`retries` → failureThreshold) | no livenessProbe |
| `container_name` | label `nslab.io/container` (what `platform.py` resolves pods and Deployments by) | |
| `image` | `<HARBOR>/<project>/<path>:<tag>` | what `push-images.sh` mirrored |
| `ulimits`, `networks` (ipam) | not mapped; warning | node defaults; pod network |

## `harness/nslab/platform.py` (NSLAB_PLATFORM=k3s)
- `exec_in` → `kubectl exec -n <ns> <pod> -- …` (pod found by label; cached until a lifecycle action busts the cache)
- `logs` / `log_facts` → `kubectl logs` (+ kubelet rotation facts instead of a docker log driver)
- `container_action(stop|kill|start|restart)` → scale the owning Deployment (found by label, works at 0 replicas) / rollout restart
- `inspect` → docker-shaped dict from the pod: Mounts (PVC claim as `<ns>/<claim>`), Config.Image / Labels / Cmd, State, pod IP
- `run_oneshot(image, cmd, volumes={"<ns>/<claim>": mount}, user, entrypoint, env)` → a throw-away Pod, waited to completion, logs returned
- `copy_out` / `copy_in` → `kubectl cp` (+ `chmod -R a+rwX` after copy-in so the engine's user can read/move the files)
- `up` / `down` → `kubectl apply -k stacks/<key>/k8s` + rollout/Job waits; `kubectl delete namespace`
- `map_port` → `k8s/ports.json`; `config.load_stack` applies `k8s_targets:` overrides and maps `advertised:` addresses too
- results → `results/k3s/<engine>/` (`util.results_root()`), displayed as `<engine> (k3s)` next to the compose rows
