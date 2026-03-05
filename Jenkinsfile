pipeline {
    agent any

    environment {
        SERVER = "ubuntu@103.65.21.198"
        IMAGE_PATH = "/data1/generated-files/vunet-images"
        HELM_PATH = "/home/vunet/launcher/static-file/helmcharts/healthwatch"
        NAMESPACE = "vsmaps"
    }

    stages {

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

        stage('Copy TAR to Server') {
            steps {
                bat """
                scp -o StrictHostKeyChecking=no healthwatch.tar %SERVER%:/tmp/
                """
            }
        }

        stage('Deploy on Server') {
            steps {
                bat """
                ssh -o StrictHostKeyChecking=no %SERVER% "sudo mv /tmp/healthwatch.tar %IMAGE_PATH%/healthwatch.tar && sudo crictl image import %IMAGE_PATH%/healthwatch.tar && helm upgrade --install healthwatch %HELM_PATH% -n %NAMESPACE% --create-namespace"
                """
            }
        }

    }

    post {
        success {
            echo 'Deployment Successful 🚀'
        }
        failure {
            echo 'Deployment Failed ❌'
        }
    }
}