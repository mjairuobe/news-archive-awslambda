# awsecr-pipeline-jenkins

Jenkins-Pipeline: Docker-Image bauen, nach Amazon ECR pushen und eine AWS-Lambda-Funktion aus dem Container-Image bereitstellen.

- `Dockerfile` – Lambda-Runtime-Image (Python 3.11)
- `function/` – Anwendungscode und `requirements.txt`
- `Jenkinsfile` – Pipeline (ECR-Login, Build, Push, Lambda-Deploy)
