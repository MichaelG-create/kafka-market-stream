US10-T1 – notes
- Used kind to create a local cluster (kind create cluster --name kafka-lab).
- Deployed nginx:1.27-alpine via a Deployment (1 replica) from nginx-deployment.yaml.
- Verified pod running with `kubectl get pods` and described it with `kubectl describe pod`.
- Exposed it as a ClusterIP service (kubectl expose deployment ...).
- First impression: Deployment manages pod lifecycle; pods are ephemeral, service is stable endpoint.
