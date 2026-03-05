pipeline {
    agent any

    environment {
        SERVER="ubuntu@103.65.21.198"
        IMAGE_PATH="/data1/generated-files/vunet-images"
        HELM_PATH="/home/ubuntu/launcher/static-files/helm-charts/healthwatch"
        NAMESPACE="vsmaps"
    }

    stages {

        stage('Checkout Code') {
            steps {
                git 'https://github.com/NiveshRamesh/Healthwatch.git'
            }
        }

        stage('Build Docker Image') {
            steps {
                bat 'docker build -t healthwatch .'
            }
        }

        stage('Save Image TAR') {
            steps {
                bat 'docker save healthwatch -o healthwatch.tar'
            }
        }

        stage('Copy Image to Server') {
            steps {
                bat 'scp -o StrictHostKeyChecking=no healthwatch.tar %SERVER%:%IMAGE_PATH%/'
            }
        }

        stage('Load Image with CRICTL') {
            steps {
                bat 'ssh %SERVER% "sudo crictl image import %IMAGE_PATH%/healthwatch.tar"'
            }
        }

        stage('Redeploy Helm Chart') {
            steps {
                bat '''
                ssh %SERVER% "
                helm uninstall healthwatch -n %NAMESPACE% || true
                cd %HELM_PATH%
                helm install healthwatch . -n %NAMESPACE%
                "
                '''
            }
        }

    }
}