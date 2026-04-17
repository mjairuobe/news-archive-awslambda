// RSS-Lambda: Docker-Image bauen, nach ECR pushen, Lambda-Funktion aus dem Image bereitstellen.

pipeline {
    agent any

    environment {
        AWS_REGION = 'eu-central-1'
        ECR_REGISTRY = '423623826655.dkr.ecr.eu-central-1.amazonaws.com'
        ECR_REPOSITORY = 'dflowp/news-archive-lambda'
        LAMBDA_FUNCTION_NAME = 'rss-lambda'
        LAMBDA_ROLE_ARN = 'arn:aws:iam::423623826655:role/service-role/rss-lamda-role-if7gg0ib'
        IMAGE_URI = "${ECR_REGISTRY}/${ECR_REPOSITORY}:latest"
    }

    options {
        timestamps()
        timeout(time: 60, unit: 'MINUTES')
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('AWS ECR Login') {
            steps {
                sh '''
                    set -e
                    aws ecr get-login-password --region ${AWS_REGION} | docker login --username AWS --password-stdin ${ECR_REGISTRY}
                '''
            }
        }

        stage('Docker build') {
            steps {
                sh '''
                    set -e
                    # Build-Kontext: Repo-Root; Quellen unter function/, Dockerfile im Root
                    docker build -f Dockerfile -t ${ECR_REPOSITORY}:latest .
                    docker tag ${ECR_REPOSITORY}:latest ${IMAGE_URI}
                '''
            }
        }

        stage('AWS ECR push') {
            steps {
                sh '''
                    set -e
                    docker push ${IMAGE_URI}
                '''
            }
        }

        stage('Deploy Lambda from image') {
            steps {
                sh '''
                    set -e
                    if aws lambda get-function --function-name "${LAMBDA_FUNCTION_NAME}" --region "${AWS_REGION}" >/dev/null 2>&1; then
                        echo "Updating existing Lambda ${LAMBDA_FUNCTION_NAME}..."
                        aws lambda update-function-code \
                            --function-name "${LAMBDA_FUNCTION_NAME}" \
                            --image-uri "${IMAGE_URI}" \
                            --region "${AWS_REGION}"
                    else
                        echo "Creating Lambda ${LAMBDA_FUNCTION_NAME} from image..."
                        aws lambda create-function \
                            --function-name "${LAMBDA_FUNCTION_NAME}" \
                            --package-type Image \
                            --code "ImageUri=${IMAGE_URI}" \
                            --role "${LAMBDA_ROLE_ARN}" \
                            --region "${AWS_REGION}"
                    fi
                '''
            }
        }
    }
}
