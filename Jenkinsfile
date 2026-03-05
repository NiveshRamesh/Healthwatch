pipeline {
    agent any

    environment {
        SERVER = "ubuntu@103.65.21.198"
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
                bat 'scp -o StrictHostKeyChecking=no healthwatch.tar %SERVER%:/tmp/'
            }
        }

        stage('Deploy on Server') {
            steps {
                bat 'ssh -o StrictHostKeyChecking=no %SERVER% "bash /home/ubuntu/deploy.sh"'
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